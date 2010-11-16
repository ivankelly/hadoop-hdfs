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
import java.io.IOException;
import java.util.Collection;
import java.util.ArrayList;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import org.apache.hadoop.hdfs.server.common.Storage; 
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory; 
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;

import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;

public class BackupNodePersistenceManager extends PersistenceManager {
  // Names of the journal spool directory and the spool file
  private static final String STORAGE_JSPOOL_DIR = "jspool";
  private static final String STORAGE_JSPOOL_FILE = 
                                              NameNodeFile.EDITS_NEW.getName();

  /** Backup input stream for loading edits into memory */
  private EditLogBackupInputStream backupInputStream;
  /** Is journal spooling in progress */
  volatile JSpoolState jsState;

  static enum JSpoolState {
    OFF,
    INPROGRESS,
    WAIT;
  }

  public BackupNodePersistenceManager(Configuration conf) throws IOException {
    super(conf);
    
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
    storage.checkpointTime = 0L;
    
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
  void loadCheckpoint(CheckpointSignature sig) throws IOException {
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
    storage.checkpointTime = sig.checkpointTime;
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
    for(StorageDirectory sd : storageDirs) {
      // rename current to lastcheckpoint.tmp
      storage.moveCurrent(sd);
    }
  }

  
  /**
   * Save meta-data into fsimage files.
   * and create empty edits.
   */
  void saveCheckpoint() throws IOException {
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

}
