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

import org.apache.hadoop.conf.Configuration;

import java.util.Collection;
import java.util.ArrayList;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory; 
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;

public class CheckpointingPersistenceManager extends PersistenceManager {
  public CheckpointingPersistenceManager(Configuration conf) throws IOException {
    super(conf);
  }

  /**
     Get a list of the configured image files on the server
  */
  public Collection<File> getImageFilenames() {
    ArrayList<File> list = new ArrayList<File>();

    for ( StorageDirectory sd : storage.iterable(NameNodeDirType.IMAGE) ) {
      list.add(storage.getImageFile(sd));
    }
    return list;
  }

  /**
    Get a list of the configured edit log files on the server
  */
  public Collection<File> getEditLogFilenames() {
    ArrayList<File> list = new ArrayList<File>();

    for ( StorageDirectory sd : storage.iterable(NameNodeDirType.IMAGE) ) {
      list.add(storage.getEditFile(sd));
    }
    return list;
  }

  /**
     @return the size of the checkpoint on disk
  */
  public long getCheckpointSize() {
    return image.getFsImageName().length();
  }
}