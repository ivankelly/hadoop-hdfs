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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import static org.apache.hadoop.hdfs.server.common.Util.now;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.conf.Configuration;

/**
 * Extension of FSImage for the backup node.
 * This class handles the setup of the journaling 
 * spool on the backup namenode.
 */
@InterfaceAudience.Private
public class BackupImage extends FSImage {
  // Names of the journal spool directory and the spool file
  private static final String STORAGE_JSPOOL_DIR = "jspool";
  private static final String STORAGE_JSPOOL_FILE =
    NNStorage.NameNodeFile.EDITS_NEW.getName();

  /** Backup input stream for loading edits into memory */
  private EditLogBackupInputStream backupInputStream;

  /** Is journal spooling in progress */
  volatile JSpoolState jsState;
  private long lastAppliedTxId = 0;

  static enum JSpoolState {
    OFF,
    INPROGRESS,
    WAIT;
  }


  /**
   * Place-holder for a txid that still needs to be addressed
   * in HDFS-1073 branch before merging into trunk.
   */
  private static final long TODO_TXID = 0xDEADBEEF;

  /**
   * Construct a backup image.
   * @param conf Configuration
   * @throws IOException if storage cannot be initialised.
   */
  BackupImage(Configuration conf) throws IOException {
    super(conf);
    storage.setDisablePreUpgradableLayoutCheck(true);
    jsState = JSpoolState.OFF;
  }

  /**
   * Analyze backup storage directories for consistency.<br>
   * Recover from incomplete checkpoints if required.<br>
   * Read VERSION and fstime files if exist.<br>
   * Do not load image or edits.
   *
   * @throws IOException if the node should shutdown.
   */
  void recoverCreateRead() throws IOException {
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
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
   * Reset storage directories.
   * <p>
   * Unlock the storage.
   * Rename <code>current</code> to <code>lastcheckpoint.tmp</code>
   * and recreate empty <code>current</code>.
   * @throws IOException
   */
  synchronized void reset() throws IOException {
    // reset NameSpace tree
    FSDirectory fsDir = getFSNamesystem().dir;
    fsDir.reset();

    // unlock, close and rename storage directories
    storage.unlockAll();
    
    // recover from unsuccessful checkpoint if necessary
    recoverCreateRead();
    // rename and recreate
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      // rename current to lastcheckpoint.tmp
      storage.moveCurrent(sd);
    }
  }

  /**
   * Load checkpoint from local files only if the memory state is empty.<br>
   * Set new checkpoint time received from the name-node.<br>
   * Move <code>lastcheckpoint.tmp</code> to <code>previous.checkpoint</code>.
   * @throws IOException
   */
  void loadCheckpoint(CheckpointSignature sig) throws IOException {
    // load current image and journal if it is not in memory already
    if(!editLog.isOpen())
      editLog.startLogSegment(TODO_TXID);

    // set storage fields
    storage.setStorageInfo(sig);

    FSDirectory fsDir = getFSNamesystem().dir;
    if(fsDir.isEmpty()) {
      Iterator<StorageDirectory> itImage
        = storage.dirIterator(NameNodeDirType.IMAGE);
      Iterator<StorageDirectory> itEdits
        = storage.dirIterator(NameNodeDirType.EDITS);
      if(!itImage.hasNext() || ! itEdits.hasNext())
        throw new IOException("Could not locate checkpoint directories");
      StorageDirectory sdName = itImage.next();
      StorageDirectory sdEdits = itEdits.next();

      getFSDirectoryRootLock().writeLock();
      try { // load image under rootDir lock
        File imageFile = null; // TODO
        MD5Hash expectedMD5 = MD5FileUtils.readStoredMd5ForFile(imageFile);
        loadFSImage(imageFile, expectedMD5);
      } finally {
        getFSDirectoryRootLock().writeUnlock();
      }
      List<File> editsFiles =
        FSImageOldStorageInspector.getEditsInStorageDir(sdEdits);
      loadEdits(new FilePreTransactionJournalManager(sdEdits, editsFiles));

      lastAppliedTxId = getEditLog().getLastWrittenTxId();
    }
  }

  /**
   * Save meta-data into fsimage files.
   * and create empty edits.
   */
  void saveCheckpoint() throws IOException {
    saveNamespace();
  }

  private FSDirectory getFSDirectoryRootLock() {
    return getFSNamesystem().dir;
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
  synchronized void journal(int length, byte[] data) throws IOException {
    assert backupInputStream.length() == 0 : "backup input stream is not empty";
    try {
      switch(jsState) {
        case WAIT:
        case OFF:
          // wait until spooling is off
          waitSpoolEnd();
          // update NameSpace in memory
          backupInputStream.setBytes(data);
          FSEditLogLoader logLoader = new FSEditLogLoader(namesystem);
          int logVersion = storage.getLayoutVersion();
          BufferedInputStream bin = new BufferedInputStream(backupInputStream);
          DataInputStream in = new DataInputStream(bin);
          Checksum checksum = null;
          if (logVersion <= -28) { // support fsedits checksum
            checksum = FSEditLog.getChecksum();
            in = new DataInputStream(new CheckedInputStream(bin, checksum));
          }
          logLoader.loadEditRecords(logVersion, in, checksum, true,
                                    lastAppliedTxId + 1);
          getFSNamesystem().dir.updateCountForINodeWithQuota(); // inefficient!
          break;
        case INPROGRESS:
          break;
      }
      // write to files
      editLog.logEdit(length, data);
      editLog.logSync();
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
  synchronized void startJournalSpool(NamenodeRegistration nnReg)
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
    for (Iterator<StorageDirectory> it
           = storage.dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
      StorageDirectory sd = it.next();
      File jsDir = getJSpoolDir(sd);
      if (!jsDir.exists() && !jsDir.mkdirs()) {
        throw new IOException("Mkdirs failed to create "
                              + jsDir.getCanonicalPath());
      }
      // create edit file if missing
      /*File eFile = storage.getEditFile(sd); TODO
      if(!eFile.exists()) {
        editLog.createEditLogFile(eFile);
      }*/
    }

    if(!editLog.isOpen())
      editLog.startLogSegment(TODO_TXID);

    // create streams pointing to the journal spool files
    // subsequent journal records will go directly to the spool
// TODO    editLog.divertFileStreams(STORAGE_JSPOOL_DIR + "/" + STORAGE_JSPOOL_FILE);

    // set up spooling
    if(backupInputStream == null)
      backupInputStream = new EditLogBackupInputStream(nnReg.getAddress());
    jsState = JSpoolState.INPROGRESS;
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
  void convergeJournalSpool() throws IOException {
    Iterator<StorageDirectory> itEdits
      = storage.dirIterator(NameNodeDirType.EDITS);
    if(! itEdits.hasNext())
      throw new IOException("Could not locate checkpoint directories");
    StorageDirectory sdEdits = itEdits.next();
    int numEdits = 0;
    File jSpoolFile = getJSpoolFile(sdEdits);
    long startTime = now();
    if(jSpoolFile.exists()) {
      // load edits.new
      EditLogFileInputStream edits = new EditLogFileInputStream(jSpoolFile);
      BufferedInputStream bin = new BufferedInputStream(edits);
      DataInputStream in = new DataInputStream(bin);
      FSEditLogLoader logLoader = new FSEditLogLoader(namesystem);
      int logVersion = logLoader.readLogVersion(in);
      Checksum checksum = null;
      if (logVersion <= -28) { // support fsedits checksum
        checksum = FSEditLog.getChecksum();
        in = new DataInputStream(new CheckedInputStream(bin, checksum));
      }
      int loaded = logLoader.loadEditRecords(logVersion, in, checksum, false,
          lastAppliedTxId + 1);

      lastAppliedTxId += loaded;
      numEdits += loaded;

      // first time reached the end of spool
      jsState = JSpoolState.WAIT;
      loaded = logLoader.loadEditRecords(logVersion, in, checksum,
                                         true, lastAppliedTxId + 1);
      numEdits += loaded;
      lastAppliedTxId += loaded;

      getFSNamesystem().dir.updateCountForINodeWithQuota();
      edits.close();
    }

    FSImage.LOG.info("Edits file " + jSpoolFile.getCanonicalPath()
        + " of size " + jSpoolFile.length() + " edits # " + numEdits
        + " loaded in " + (now()-startTime)/1000 + " seconds.");

    // rename spool edits.new to edits making it in sync with the active node
    // subsequent journal records will go directly to edits
    // TODO editLog.revertFileStreams(STORAGE_JSPOOL_DIR + "/" + STORAGE_JSPOOL_FILE);

    // write version file
    // TODO resetVersion(storage.getImageDigest());

    // wake up journal writer
    synchronized(this) {
      jsState = JSpoolState.OFF;
      notifyAll();
    }

    // Rename lastcheckpoint.tmp to previous.checkpoint
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      storage.moveLastCheckpoint(sd);
    }
  }
}
