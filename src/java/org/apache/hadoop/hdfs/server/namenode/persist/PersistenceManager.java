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

import org.apache.hadoop.hdfs.server.namenode.NNStorage;

public class PersistenceManager {
  
  protected FSImage image;
  protected FSEditLog editlog;
  protected NNStorage storage;
  
  /* Constructor */
  public PersistenceManager(Configuration conf){
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

  /**
     Implement functionallity used by the command line options
  */
  public static void upgrade() {
  }

  public static void rollback() {
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
  }

  /**
     Load the latest version of the FSNameSystem from disk
     w Interfaces
  */
  public void load() throws IOException {
        // format before starting up if requested
    /*    if (startOpt == StartupOption.FORMAT) {
      fsImage.setStorageDirectories(dataDirs, editsDirs);
      fsImage.format();
      startOpt = StartupOption.REGULAR;
    }
    try {
      if (fsImage.recoverTransitionRead(dataDirs, editsDirs, startOpt)) {
        fsImage.saveNamespace(true);
      }
      FSEditLog editLog = fsImage.getEditLog();
      assert editLog != null : "editLog must be initialized";
      fsImage.setCheckpointDirectories(null, null);
    } catch(IOException e) {
      fsImage.close();
      throw e;
    }
    writeLock();
    try {
      this.ready = true;
      this.nameCache.initialized();
      cond.signalAll();
    } finally {
      writeUnlock();
    }
    */
    
  }
  
  public FSEditLog getLog() {
    return null; // TODO
  }
}
