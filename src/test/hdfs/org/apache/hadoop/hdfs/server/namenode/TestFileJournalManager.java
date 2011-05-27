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

import static org.junit.Assert.*;

import java.net.URI;
import java.util.Collections;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.SecurityUtil;
import org.junit.Test;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;

public class TestFileJournalManager {
  public StorageDirectory setupEdits(File f, int numrolls, int crashatroll) throws IOException {
    NNStorage storage = new NNStorage(new Configuration(),
                                      Collections.<URI>emptyList(),
                                      Collections.singletonList(f.toURI()));
    StorageDirectory sd = storage.getStorageDirectory(f.toURI());
    storage.format(sd);
    FSEditLog editlog = new FSEditLog(storage);
    
    editlog.open();
    for (int i = 0; i < numrolls; i++) {
      editlog.logGenerationStamp((long)i);
      if (i == crashatroll) {
        //try {Thread.sleep(100000);} catch (Exception e) {}
        return sd; // should leave in inprogress file
      }
      editlog.rollEditLog();
    }
    editlog.close();
    return sd;
  }

  @Test
  public void TestInprogressRecovery() throws IOException {
    File f = new File(System.getProperty("test.build.data", "build/test/data") + "/filejournaltest");
    StorageDirectory sd = setupEdits(f, 10, 5);

    FileJournalManager jm = new FileJournalManager(sd);
    assertEquals(3, jm.getNumberOfTransactions(1));
  }
  
  @Test
  public void TestInprogressRecoveryEmptyFile() throws IOException {
  }

  @Test
  public void TestInprogressRecoveryJunkFile() throws IOException {
  }

  @Test
  public void TestManyLogsWithGaps() throws IOException {
  }

  @Test
  public void TestManyLogsWithGapWhereInprogressShouldFillGap() throws IOException {
  }

  @Test
  public void TestGetNumTransactionsHalfWayThroughSegment() throws IOException {
  }
}
