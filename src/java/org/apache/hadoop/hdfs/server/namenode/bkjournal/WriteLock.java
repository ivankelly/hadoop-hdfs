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
package org.apache.hadoop.hdfs.server.namenode.bkjournal;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

import java.io.IOException;

public class WriteLock implements Watcher {
  private final ZooKeeper zkc;
  private final String lockpath;
  private String myznode = "";

  WriteLock(ZooKeeper zkc, String lockpath) throws IOException {
    this.lockpath = lockpath;

    this.zkc = zkc;
    try {
      if (zkc.exists(lockpath, false) == null) {
        zkc.create(lockpath, new byte[] {'0'}, 
                   Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    } catch (Exception e) {
      throw new IOException("Exception accessing Zookeeper", e);
    }
  }

  void acquire() throws IOException {
    try {
      myznode = zkc.create(lockpath, new byte[] {'0'}, 
                           Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    } catch (Exception e) {
      throw new IOException("Exception accessing Zookeeper", e);
    }

  }

  void release() throws IOException {
    try {
      zkc.delete(myznode, -1);
      myznode = null;
    } catch (Exception e) {
      throw new IOException("Exception accessing Zookeeper", e);
    }
  }

  public void checkWriteLock() throws IOException {
    if (!haveLock()) {
      throw new IOException("Lost writer lock");
    }
  }

  
  boolean haveLock() throws IOException {
    return myznode != null;
  }

  public void process(WatchedEvent event) {
    if (event.getState() == KeeperState.Disconnected
        || event.getState() == KeeperState.Expired) {
      myznode = null;
    } else {
      // reapply the watch
      if (myznode != null) {
        try {
          zkc.exists(myznode, this); 
        } catch (Exception e) {
          // TODO throw new IOException("Exception accessing Zookeeper", e);
        }
      }
    }
  }
}