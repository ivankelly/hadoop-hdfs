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

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;

import static org.apache.hadoop.hdfs.server.common.Util.now;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hdfs.server.namenode.NNStorage.StorageErrorListener;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;

@InterfaceAudience.Private
public class BackupStorage extends FSImage {

  /**
   */
  BackupStorage(Configuration conf, NNStorage storage) {
    super(conf,storage);
  }




  private FSDirectory getFSDirectoryRootLock() {
    return getFSNamesystem().dir;
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
          editLog.loadEditRecords(storage.getLayoutVersion(),
                    backupInputStream.getDataInputStream(), true);
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
    for(Iterator<StorageDirectory> it = 
                      storage.dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
      StorageDirectory sd = it.next();
      File jsDir = getJSpoolDir(sd);
      if (!jsDir.exists() && !jsDir.mkdirs()) {
        throw new IOException("Mkdirs failed to create "
                              + jsDir.getCanonicalPath());
      }
      // create edit file if missing
      File eFile = getEditFile(sd);
      if(!eFile.exists()) {
        editLog.createEditLogFile(eFile);
      }
    }

    if(!editLog.isOpen())
      editLog.open();

    // create streams pointing to the journal spool files
    // subsequent journal records will go directly to the spool
    editLog.divertFileStreams(STORAGE_JSPOOL_DIR + "/" + STORAGE_JSPOOL_FILE);
    storage.setCheckpointState(CheckpointStates.ROLLED_EDITS);

    // set up spooling
    if(backupInputStream == null)
      backupInputStream = new EditLogBackupInputStream(nnReg.getAddress());
    jsState = JSpoolState.INPROGRESS;
  }

  synchronized void setCheckpointTime(int length, byte[] data)
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
  void convergeJournalSpool() throws IOException {
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
      numEdits += editLog.loadFSEdits(in, false);
  
      // first time reached the end of spool
      jsState = JSpoolState.WAIT;
      numEdits += editLog.loadEditRecords(storage.getLayoutVersion(), in, true);
      getFSNamesystem().dir.updateCountForINodeWithQuota();
      edits.close();
    }

    FSImage.LOG.info("Edits file " + jSpoolFile.getCanonicalPath() 
        + " of size " + jSpoolFile.length() + " edits # " + numEdits 
        + " loaded in " + (now()-startTime)/1000 + " seconds.");

    // rename spool edits.new to edits making it in sync with the active node
    // subsequent journal records will go directly to edits
    editLog.revertFileStreams(STORAGE_JSPOOL_DIR + "/" + STORAGE_JSPOOL_FILE);

    // write version file
    resetVersion(false);

    // wake up journal writer
    synchronized(this) {
      jsState = JSpoolState.OFF;
      notifyAll();
    }

    // Rename lastcheckpoint.tmp to previous.checkpoint
    for(StorageDirectory sd : storageDirs) {
      moveLastCheckpoint(sd);
    }
  }
}
