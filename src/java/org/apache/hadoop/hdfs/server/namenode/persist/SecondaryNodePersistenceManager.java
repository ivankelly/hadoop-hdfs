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
import java.net.URISyntaxException;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SecondaryNodePersistenceManager extends CheckpointingPersistenceManager {
  public static final Log LOG = LogFactory.getLog(SecondaryNodePersistenceManager.class.getName());

  private String DEFAULT_NAMENODE_CHECKPOINT_DIR = "/tmp/hadoop/dfs/namesecondary";

  /**
   * Create the SecondaryNodePersistenceManager
   *
   * Analyze checkpoint directories.
   * Create directories if they do not exist.
   * Recover from an unsuccessful checkpoint is necessary. 
   * 
   * @throws IOException
   */
  public SecondaryNodePersistenceManager(Configuration conf, NNStorage storage) throws IOException {
    super(conf, storage);

    setupDirectories();
  }

  private void setupDirectories() throws IOException {
    try {
      Collection<String> dirNames = conf.getStringCollection(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);
      if (dirNames.size() == 0) {
	storage.addStorageDirectory(new URI(DEFAULT_NAMENODE_CHECKPOINT_DIR), NNStorage.NameNodeDirType.IMAGE);
      } else {
	for (String s : dirNames) {
	  storage.addStorageDirectory(new URI(s), NNStorage.NameNodeDirType.IMAGE);
	}
      }
      
      dirNames = conf.getStringCollection(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY);
      if (dirNames.size() == 0) {
	storage.addStorageDirectory(new URI(DEFAULT_NAMENODE_CHECKPOINT_DIR), NNStorage.NameNodeDirType.EDITS);
      } else {
	for (String s : dirNames) {
	  storage.addStorageDirectory(new URI(s), NNStorage.NameNodeDirType.EDITS);
	}
      }
    } catch (URISyntaxException use) {
      String msg = "Invalidly formed URIs for either " + 
	DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY +
	" or " + DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY;
      LOG.error(msg);
      throw new IOException(msg, use);
    }
    
    for (StorageDirectory sd : storage ) {
      boolean isAccessible = true;
      try { // create directories if don't exist yet
        if(!sd.getRoot().mkdirs()) {
          // do nothing, directory is already created
        }
      } catch(SecurityException se) {
        isAccessible = false;
      }
      if(!isAccessible)
        throw new InconsistentFSStateException(sd.getRoot(),
                                               "cannot access checkpoint directory.");
      StorageState curState;
      try {
        curState = sd.analyzeStorage(HdfsConstants.StartupOption.REGULAR);
        // sd is locked but not opened
        switch(curState) {
        case NON_EXISTENT:
          // fail if any of the configured checkpoint dirs are inaccessible 
          throw new InconsistentFSStateException(sd.getRoot(),
                                                 "checkpoint directory does not exist or is not accessible.");
        case NOT_FORMATTED:
          break;  // it's ok since initially there is no current and VERSION
        case NORMAL:
          break;
        default:  // recovery is possible
          sd.doRecover(curState);
        }
      } catch (IOException ioe) {
        sd.unlock();
        throw ioe;
      }
    }
  }
  
  /**
   * Prepare directories for a new checkpoint.
   * <p>
   * Rename <code>current</code> to <code>lastcheckpoint.tmp</code>
   * and recreate <code>current</code>.
   * @throws IOException
   */
  /*  @Override */
  public void startCheckpoint() throws IOException {
    storage.unlockAll();
    editlog.close();
    setupDirectories();

    for(StorageDirectory sd : storage) {
      storage.moveCurrent(sd);
    }
  }
  
  /* @Override*/
  public void endCheckpoint() throws IOException {
    for(StorageDirectory sd : storage) {
      storage.moveLastCheckpoint(sd);
    }
  }

  /* @Override*/
  public boolean isPreUpgradableLayout(StorageDirectory sd) throws IOException {
      return false;
  }

  /**
   * Merge image and edits, and verify consistency with the signature.
   */
  public void merge(CheckpointSignature sig) throws IOException {
    // Ugly, ugly, ugly, TODO explore why this is even needed
    storage.layoutVersion = -1; // to avoid assert in loadFSImage() TODO 

    load();

    sig.validateStorageInfo(storage);

    save();
  }

  /**
     Update the storage directory to reflect the checkpoint and ctime
     of the image stored in it
  */
  public void updateStorageTimes(long cTime, long checkpointTime) {
    storage.setCTime(cTime);
    storage.setCheckpointTime(checkpointTime);
  }
}

