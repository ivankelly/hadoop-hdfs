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

import org.apache.hadoop.hdfs.server.common.Storage; //TODO remove when we have concrete
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import java.io.IOException;

import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;

import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile; 


import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.util.Map;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hdfs.server.common.Storage; 
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory; 
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;

public class PersistenceManager {
  public static final Log LOG = LogFactory.getLog(Storage.class.getName());

  protected Configuration conf;
  protected FSImage image;
  protected FSEditLog editlog;
  protected NNStorage storage;
  
  /* Constructor */
  public PersistenceManager(Configuration conf) {
    conf = conf;
    // TODO 
    //		storage = new NNStorage(conf);
    //fsi = new FSImage(conf,null);//storage);
  }
  
  /**
     For managing checkpoints
  */
  public void startCheckpoint() throws IOException {
  }

  public void endCheckpoint() throws IOException {
  }

  public void importCheckpoint() throws IOException {
    storage.initializeDirectories( getStartupOption() );

    image.doImportCheckpoint();
  }

  /**
     Implement functionallity used by the command line options
  */
  public void upgrade() throws IOException {
    storage.initializeDirectories( getStartupOption() );

    image.doUpgrade();
  }

  public void rollback() throws IOException {
    storage.initializeDirectories( getStartupOption() );
    
    image.doRollback();
  }

  public static void finalizeUpgrade() {

  }

  public static void format() {
  }

  /**
     Save the contents of FSNameSystem to disk
     Fundamentally just dumps mem and resets the edit log
  */
  public void save() throws IOException {
    assert editlog != null : "editLog must be initialized";
    editlog.close();

    image.saveNamespace(true);

    if(!editlog.isOpen()) {
      editlog.open();
    }
  }

  /**
     Load the latest version of the FSNameSystem from disk
     w Interfaces
  */
  public void load() throws IOException {
    try {
      // FIXME where do i get the dataDirs? editsDirs? should recoverTransitionRead even be a method
      storage.initializeDirectories( getStartupOption() );

      boolean needToSave = false;
      assert editlog != null : "editLog must be initialized";
      NNStorage.LoadDirectory imagedir = image.findLatestImageDirectory();
      NNStorage.LoadDirectory editsdir = editlog.findLatestEditsDirectory();

      needToSave |= imagedir.getNeedToSave() || editsdir.getNeedToSave();

      long imgtime = storage.readCheckpointTime(imagedir.getDirectory());
      long editstime = storage.readCheckpointTime(editsdir.getDirectory());

      // Make sure we are loading image and edits from same checkpoint
      if (imgtime > editstime
	  && imagedir.getDirectory() != editsdir.getDirectory()
	  && imagedir.getDirectory().getStorageDirType() == NameNodeDirType.IMAGE
	  && editsdir.getDirectory().getStorageDirType() == NameNodeDirType.EDITS) {
	// This is a rare failure when NN has image-only and edits-only
	// storage directories, and fails right after saving images,
	// in some of the storage directories, but before purging edits.
	// See -NOTE- in saveNamespace().
	LOG.error("This is a rare failure scenario!!!");
	LOG.error("Image checkpoint time " + imgtime +
		  " > edits checkpoint time " + editstime);
	LOG.error("Name-node will treat the image as the latest state of " +
		  "the namespace. Old edits will be discarded.");
      } else if (editstime != imgtime) {
	throw new IOException("Inconsistent storage detected, " +
			      "image and edits checkpoint times do not match. " +
			      "image checkpoint time = " + imgtime +
			      "edits checkpoint time = " + editstime);
      }
      
      // Recover from previous interrupted checkpoint, if any
      needToSave |= storage.recoverInterruptedCheckpoint(imagedir.getDirectory(), editsdir.getDirectory());
      
      long startTime = now();
      long imageSize = storage.getImageFile(imagedir.getDirectory(), NameNodeFile.IMAGE).length();
    
      //
      // Load in bits
      //
      imagedir.getDirectory().read();
      needToSave |= image.loadFSImage(storage.getImageFile(imagedir.getDirectory(), NameNodeFile.IMAGE));
      LOG.info("Image file of size " + imageSize + " loaded in " 
	       + (now() - startTime)/1000 + " seconds.");
    
      // Load latest edits
      if (imgtime > editstime) {
	// the image is already current, discard edits
	needToSave |= true;
      } else { // latestNameCheckpointTime == latestEditsCheckpointTime 
	needToSave |= editlog.loadEdits(editsdir.getDirectory()) > 0;
      }

      if (needToSave) {
	save();
      }

      image.setCheckpointDirectories(null, null);
    } catch(IOException e) {
      image.close();
      throw e;
    }

    /* FIXME understand and fix this locking 
    storage.writeLock();
    try {
      this.ready = true;
      this.nameCache.initialized();
      cond.signalAll();
    } finally {
      storage.writeUnlock();
    }
    */
  }
  
  public FSEditLog getLog() {
    return editlog;
  }

  /**
     Some methods still require. TODO Fix this
  */
  private StartupOption getStartupOption() {
    return StartupOption.valueOf(conf.get("dfs.namenode.startup",
					  StartupOption.REGULAR.toString()));
  }
}
