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

import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import java.util.List;
import java.io.File;
import java.io.IOException;

class FileJournalFactory extends JournalFactory {
  private NNStorage storage;
  private StorageDirectory storageDirectory;

  public FileJournalFactory(Configuration conf, URI uri, 
                            NNStorage storage,
                            StorageDirectory sd)
      throws IllegalArgumentException {
    super(conf, uri);
    this.storage = storage;
    this.storageDirectory = sd;
  }

  /**
   * Format does nothing for file based journals as the directories could
   * be shared for image storage, so formatting of the directories is left up to 
   * FSImage
   */
  void format () throws IOException {
    EditLogOutputStream eStream = getOutputStream();
    eStream.create();
    eStream.close();
    return;
  }

  
  // // these will be replaced by a single roll() after 1073
  // // or possibly nothing at all
  // /** 
  //  * Start rolling.
  //  */
  // void startRoll() throws IOException {}
  // /**
  //  * @return true if factory is currently rolli
  // boolean isRolling() { return false; }
  // void endRoll() throws IOException {}
  
  // Streams
  EditLogInputStream getInputStream()
      throws IOException {
    return null;
  }

  EditLogOutputStream getOutputStream()
      throws IOException {
    EditLogOutputStream eStream = new EditLogFileOutputStream(
        storageDirectory,
        FileJournalFactory.getEditFile(storageDirectory),
        //sizeOutputFlushBuffer);
        512*1024); // TODOIK
    return eStream;
  }

  static File getEditFile(StorageDirectory sd) {
    return NNStorage.getEditFile(sd);
  }
  
  static File getEditNewFile(StorageDirectory sd) {
    return NNStorage.getEditNewFile(sd);
  }
}