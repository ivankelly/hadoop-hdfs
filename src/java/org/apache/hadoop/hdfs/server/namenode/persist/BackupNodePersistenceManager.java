/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.persist;

import java.io.File;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

import java.net.URI;

import static org.apache.hadoop.hdfs.server.common.Util.now;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.common.Storage; 
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory; 
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;

import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.EditLogBackupInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;

import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

//import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BackupNodePersistenceManager extends CheckpointingPersistenceManager {
  protected static final Log LOG = LogFactory.getLog(BackupNodePersistenceManager.class.getName());

  // Names of the journal spool directory and the spool file
  private static final String STORAGE_JSPOOL_DIR = "jspool";
  private static final String STORAGE_JSPOOL_FILE = NNStorage.NameNodeFile.EDITS_NEW.getName();

  /** Backup input stream for loading edits into memory */
  private EditLogBackupInputStream backupInputStream;
  /** Is journal spooling in progress */
  volatile JSpoolState jsState;

  static enum JSpoolState {
    OFF,
    INPROGRESS,
    WAIT;
  }

  public BackupNodePersistenceManager(Configuration conf, NNStorage storage) throws IOException {
    super(conf, storage);
    
    jsState = JSpoolState.OFF;
  }

  /**
   * Analyze backup storage directories for consistency.<br>
   * Recover from incomplete checkpoints if required.<br>
   * Read VERSION and fstime files if exist.<br>
   * Do not load image or edits.
   * 
   * @param imageDirs list of image directories as URI.
   * @param editsDirs list of edits directories URI.
   * @throws IOException if the node should shutdown.
   */
  public void recoverCreateRead() throws IOException {
    storage.setCheckpointTimeNoWrite(0L);
    
    for(StorageDirectory sd: storage) {
      StorageState curState;
      try {
        curState = sd.analyzeStorage(HdfsConstants.StartupOption.REGULAR);
        // sd is locked but not opened
        switch(curState) {
        case NON_EXISTENT:
          // fail if any of the configured storage dirs are inaccessible 
          throw new InconsistentFSStateException(sd.getRoot(),
						 "checkpoint directory does not exist or is not accessible.");
        case NOT_FORMATTED:
          // for backup node all directories may be unformatted initially
          LOG.info("Storage directory " + sd.getRoot() + " is not formatted.");
          LOG.info("Formatting ...");
          sd.clearDirectory(); // create empty current
          break;
        case NORMAL:
          break;
        default:  // recovery is possible
          sd.doRecover(curState);
        }
        if(curState != StorageState.NOT_FORMATTED) {
          sd.read(); // read and verify consistency with other directories
        }
      } catch(IOException ioe) {
        sd.unlock();
        throw ioe;
      }
    }
  }
  
  
  /**
   * Load checkpoint from local files only if the memory state is empty.<br>
   * Set new checkpoint time received from the name-node.<br>
   * Move <code>lastcheckpoint.tmp</code> to <code>previous.checkpoint</code>.
   * @throws IOException
   */
  public void loadCheckpoint(CheckpointSignature sig) throws IOException {
    // load current image and journal if it is not in memory already
    if(!editlog.isOpen())
      editlog.open();

    FSDirectory fsDir = namesystem.dir;
    if(fsDir.isEmpty()) {
      fsDir.writeLock();
      try { // load image under rootDir lock
        this.load();
      } finally {
        fsDir.writeUnlock();
      }
    }

    // set storage fields
    storage.setStorageInfo(sig);
    storage.setImageDigest(sig.imageDigest);
    storage.setCheckpointTimeNoWrite(sig.checkpointTime);
  }

  /**
   * Reset storage directories.
   * <p>
   * Unlock the storage.
   * Rename <code>current</code> to <code>lastcheckpoint.tmp</code>
   * and recreate empty <code>current</code>.
   * @throws IOException
   */
  public synchronized void reset() throws IOException {
    // reset NameSpace tree
    FSDirectory fsDir = namesystem.dir;
    fsDir.reset();

    // unlock, close and rename storage directories
    storage.unlockAll();

    // recover from unsuccessful checkpoint if necessary
    recoverCreateRead();

    // rename and recreate
    for(StorageDirectory sd : storage) {
      // rename current to lastcheckpoint.tmp
      storage.moveCurrent(sd);
    }
  }

  
  /**
   * Save meta-data into fsimage files.
   * and create empty edits.
   */
  public void saveCheckpoint() throws IOException {
    assert editlog != null : "editLog must be initialized";
    editlog.close();

    image.saveNamespace(false);
    
    editlog.createEditLogFiles();
    
    if(!editlog.isOpen()) {
      editlog.open();
    }
  }
  
  static File getJSpoolDir(StorageDirectory sd) {
    return new File(sd.getRoot(), STORAGE_JSPOOL_DIR);
  }

  static File getJSpoolFile(StorageDirectory sd) {
    return new File(getJSpoolDir(sd), STORAGE_JSPOOL_FILE);
  }
  
  /**
   * Journal writer journals new meta-data state.
   * <ol>
   * <li> If Journal Spool state is OFF then journal records (edits)
   * are applied directly to meta-data state in memory and are written 
   * to the edits file(s).</li>
   * <li> If Journal Spool state is INPROGRESS then records are only 
   * written to edits.new file, which is called Spooling.</li>
   * <li> Journal Spool state WAIT blocks journaling until the
   * Journal Spool reader finalizes merging of the spooled data and
   * switches to applying journal to memory.</li>
   * </ol>
   * @param length length of data.
   * @param data serialized journal records.
   * @throws IOException
   * @see #convergeJournalSpool()
   */
  public synchronized void journal(int length, byte[] data) throws IOException {
    assert backupInputStream.length() == 0 : "backup input stream is not empty";
    try {
      switch(jsState) {
        case WAIT:
        case OFF:
          // wait until spooling is off
          waitSpoolEnd();
          // update NameSpace in memory
          backupInputStream.setBytes(data);

          // HDFS-1462
	  FSEditLogLoader logLoader = new FSEditLogLoader(namesystem);
          //logLoader.loadEditRecords(getLayoutVersion(),
	  //          backupInputStream.getDataInputStream(), true);
	  ///////////////////////////////////////////////////////////
	  editlog.loadEditRecords(storage.getLayoutVersion(),
                    backupInputStream.getDataInputStream(), true);

          namesystem.dir.updateCountForINodeWithQuota(); // inefficient!
          break;
        case INPROGRESS:
          break;
      }
      // write to files
      editlog.logEdit(length, data);
      editlog.logSync();
    } finally {
      backupInputStream.clear();
    }
  }

  private synchronized void waitSpoolEnd() {
    while(jsState == JSpoolState.WAIT) {
      try {
        wait();
      } catch (InterruptedException  e) {}
    }
    // now spooling should be off, verifying just in case
    assert jsState == JSpoolState.OFF : "Unexpected JSpool state: " + jsState;
  }

  /**
   * Start journal spool.
   * Switch to writing into edits.new instead of edits.
   * 
   * edits.new for spooling is in separate directory "spool" rather than in
   * "current" because the two directories should be independent.
   * While spooling a checkpoint can happen and current will first
   * move to lastcheckpoint.tmp and then to previous.checkpoint
   * spool/edits.new will remain in place during that.
   */
  public synchronized void startJournalSpool(NamenodeRegistration nnReg)
    throws IOException {
    switch(jsState) {
    case OFF:
      break;
    case INPROGRESS:
      return;
    case WAIT:
      waitSpoolEnd();
    }
    
    // create journal spool directories
    for(StorageDirectory sd : storage.iterable(NameNodeDirType.EDITS)) {
      File jsDir = getJSpoolDir(sd);
      if (!jsDir.exists() && !jsDir.mkdirs()) {
        throw new IOException("Mkdirs failed to create "
                              + jsDir.getCanonicalPath());
      }
      // create edit file if missing
      File eFile = storage.getEditFile(sd);
      if(!eFile.exists()) {
        editlog.createEditLogFile(eFile);
      }
    }

    if(!editlog.isOpen()) {
      editlog.open();
    }

    // create streams pointing to the journal spool files
    // subsequent journal records will go directly to the spool
    editlog.divertFileStreams(STORAGE_JSPOOL_DIR + "/" + STORAGE_JSPOOL_FILE);
    setCheckpointState(CheckpointStates.ROLLED_EDITS);

    // set up spooling
    if(backupInputStream == null) {
      backupInputStream = new EditLogBackupInputStream(nnReg.getAddress());
    }

    jsState = JSpoolState.INPROGRESS;
  }

  public synchronized void setCheckpointTime(int length, byte[] data)
    throws IOException {
    assert backupInputStream.length() == 0 : "backup input stream is not empty";
    try {
      // unpack new checkpoint time
      backupInputStream.setBytes(data);
      DataInputStream in = backupInputStream.getDataInputStream();
      byte op = in.readByte();
      assert op == NamenodeProtocol.JA_CHECKPOINT_TIME;
      LongWritable lw = new LongWritable();
      lw.readFields(in);
      storage.setCheckpointTime(lw.get());
    } finally {
      backupInputStream.clear();
    }
  }

  /**
   * Merge Journal Spool to memory.<p>
   * Journal Spool reader reads journal records from edits.new.
   * When it reaches the end of the file it sets {@link JSpoolState} to WAIT.
   * This blocks journaling (see {@link #journal(int,byte[])}.
   * The reader
   * <ul>
   * <li> reads remaining journal records if any,</li>
   * <li> renames edits.new to edits,</li>
   * <li> sets {@link JSpoolState} to OFF,</li> 
   * <li> and notifies the journaling thread.</li>
   * </ul>
   * Journaling resumes with applying new journal records to the memory state,
   * and writing them into edits file(s).
   */
  public void convergeJournalSpool() throws IOException {
    Iterator<StorageDirectory> itEdits = storage.dirIterator(NameNodeDirType.EDITS);

    if(! itEdits.hasNext())
      throw new IOException("Could not locate checkpoint directories");
    StorageDirectory sdEdits = itEdits.next();
    int numEdits = 0;
    File jSpoolFile = getJSpoolFile(sdEdits);
    long startTime = now();
    if(jSpoolFile.exists()) {
      // load edits.new
      EditLogFileInputStream edits = new EditLogFileInputStream(jSpoolFile);
      DataInputStream in = edits.getDataInputStream();

      //FSEditLogLoader logLoader = new FSEditLogLoader(namesystem);
      //numEdits += logLoader.loadFSEdits(in, false);
      numEdits += editlog.loadFSEdits(in, false);
      // first time reached the end of spool
      jsState = JSpoolState.WAIT;
      numEdits += editlog.loadEditRecords(storage.getLayoutVersion(), in, true);
      namesystem.dir.updateCountForINodeWithQuota();
      edits.close();
    }

    LOG.info("Edits file " + jSpoolFile.getCanonicalPath() 

        + " of size " + jSpoolFile.length() + " edits # " + numEdits 
        + " loaded in " + (now()-startTime)/1000 + " seconds.");

    // rename spool edits.new to edits making it in sync with the active node
    // subsequent journal records will go directly to edits
    editlog.revertFileStreams(STORAGE_JSPOOL_DIR + "/" + STORAGE_JSPOOL_FILE);

    // write version file
    resetVersion(false,storage.getImageDigest());

    // wake up journal writer
    synchronized(this) {
      jsState = JSpoolState.OFF;
      notifyAll();
    }

    // Rename lastcheckpoint.tmp to previous.checkpoint
    for(StorageDirectory sd : storage) {
      storage.moveLastCheckpoint(sd);
    }
  }

  public void verifyNamespaceID(NamespaceInfo nsInfo) throws IOException {
    // verify namespaceID
    if(storage.getNamespaceID() == 0) {// new backup storage
      storage.setStorageInfo(nsInfo);
    } else if(storage.getNamespaceID() != nsInfo.getNamespaceID()) {
      throw new IOException("Incompatible namespaceIDs"
                            + ": active node namespaceID = " + nsInfo.getNamespaceID()
                            + "; backup node namespaceID = " + storage.getNamespaceID());
    }
  }

  public boolean isStorageInitialized() {
    assert storage.getNumStorageDirs() > 0;
    return ! storage.getStorageDir(0).getVersionFile().exists();
  }

  public boolean isEditLogInitialized() {
    return editlog.getNumEditStreams() == 0;
  }  

  public boolean isEditLogOpen() {
    return editlog.isOpen();
  } 
  
  public void closeEditLog() {
    editlog.close();
  }

}