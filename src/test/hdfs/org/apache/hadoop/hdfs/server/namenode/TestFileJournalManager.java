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
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.FilenameFilter;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.SecurityUtil;
import org.junit.Test;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.setupEdits;
import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.readAll;
import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.AbortSpec;
import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.TXNS_PER_ROLL;
import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.TXNS_PER_FAIL;

import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

public class TestFileJournalManager {
  @Test
  public void testInprogressRecovery() throws IOException {
    File f = new File(System.getProperty("test.build.data", "build/test/data") + "/filejournaltest0");
    // abort after the 5th roll 
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()),
                                   5, new AbortSpec(5, 0));
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();

    FileJournalManager jm = new FileJournalManager(sd);
    assertEquals(5*TXNS_PER_ROLL + TXNS_PER_FAIL, jm.getNumberOfTransactions(1));
  }

  private void corruptAfterStartSegment(File f) throws IOException {
    RandomAccessFile raf = new RandomAccessFile(f, "rw");
    raf.seek(0x16); // skip version and first tranaction and a bit of next transaction
    for (int i = 0; i < 1000; i++) {
      raf.writeInt(0xdeadbeef);
    }
    raf.close();
  }

  @Test 
  public void testReadFromStream() throws IOException {
    File f = new File(System.getProperty("test.build.data", "build/test/data") + "/filejournaltest1");
    // abort after 10th roll
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()), 10, new AbortSpec(10, 0));
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();

    FileJournalManager jm = new FileJournalManager(sd);
    long expectedTotalTxnCount = TXNS_PER_ROLL*10 + TXNS_PER_FAIL;
    assertEquals(expectedTotalTxnCount, jm.getNumberOfTransactions(1));

    long skippedTxns = (3*TXNS_PER_ROLL); // skip first 3 files
    long startingTxId = skippedTxns + 1; 

    long numTransactionsToLoad = jm.getNumberOfTransactions(startingTxId);
    long numLoaded = 0;
    while (numLoaded < numTransactionsToLoad) {
      EditLogInputStream editIn = jm.getInputStream(startingTxId);
      long count = readAll(editIn, startingTxId);

      editIn.close();
      startingTxId += count;
      numLoaded += count;
    }

    assertEquals(expectedTotalTxnCount - skippedTxns, numLoaded); 
  }

  /**
   * You cannot make a request with a start transaction id which doesn't
   * match the start ID of some log segment. 
   */
  @Test(expected=IOException.class)
  public void testAskForTransactionsMidfile() throws IOException {
    File f = new File(System.getProperty("test.build.data", "build/test/data") + "/filejournaltest2");
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()), 10);
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();
    
    FileJournalManager jm = new FileJournalManager(sd);
    jm.getNumberOfTransactions(2);    
  }

  @Test
  public void testManyLogsWithGaps() throws IOException {
    File f = new File(System.getProperty("test.build.data", "build/test/data") + "/filejournaltest3");
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()), 10);
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();

    final long startGapTxId = 3*TXNS_PER_ROLL + 1;
    final long endGapTxId = 4*TXNS_PER_ROLL;
    File[] files = new File(f, "current").listFiles(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          if (name.startsWith(NNStorage.getFinalizedEditsFileName(startGapTxId, endGapTxId))) {
            return true;
          }
          return false;
        }
      });
    assertEquals(1, files.length);
    assertTrue(files[0].delete());
    
    FileJournalManager jm = new FileJournalManager(sd);
    assertEquals(startGapTxId-1, jm.getNumberOfTransactions(1));
    
    // rolled 10 times so there should be 11 files.
    assertEquals(11*TXNS_PER_ROLL - endGapTxId, 
                 jm.getNumberOfTransactions(endGapTxId+1));
  }

  @Test
  public void testManyLogsWithGapWhereInprogressInMiddle() throws IOException {
    File f = new File(System.getProperty("test.build.data", "build/test/data") + "/filejournaltest4");
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()), 10, new AbortSpec(4, 0));
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();
    
    FileJournalManager jm = new FileJournalManager(sd);
    final long startGapTxId = 4*TXNS_PER_ROLL + TXNS_PER_FAIL + 1;
    final long endGapTxId = 5*TXNS_PER_ROLL;
    assertEquals(startGapTxId - 1, jm.getNumberOfTransactions(1));

    assertEquals(11*TXNS_PER_ROLL-endGapTxId, 
                 jm.getNumberOfTransactions(endGapTxId+1)); 
  }

  @Test
  public void testManyLogsWithCorruptInprogress() throws IOException {
    File f = new File(System.getProperty("test.build.data", "build/test/data") + "/filejournaltest5");
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()), 10, new AbortSpec(10, 0));
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();

    File[] files = new File(f, "current").listFiles(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          if (name.startsWith("edits_inprogress")) {
            return true;
          }
          return false;
        }
      });
    assertEquals(files.length, 1);
    
    corruptAfterStartSegment(files[0]);

    FileJournalManager jm = new FileJournalManager(sd);
    assertEquals(10*TXNS_PER_ROLL+1, 
                 jm.getNumberOfTransactions(1)); 
  }

  /*  @Test
  public void testManyLogsWithEmptyInprogress() throws IOException {
    File f = new File(System.getProperty("test.build.data", "build/test/data") + "/filejournaltest6");
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()), 10, new AbortSpec(10, 0));
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();

    File[] files = new File(f, "current").listFiles(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          if (name.startsWith("edits_inprogress")) {
            return true;
          }
          return false;
        }
      });
    assertEquals(files.length, 1);
    files[0].delete();
    files[0].createNewFile();
    
    FileJournalManager jm = new FileJournalManager(sd);
    assertEquals(10*TXNS_PER_ROLL, 
                 jm.getNumberOfTransactions(1)); 
                 }*/
  
  /*
  @Test
  public void TestInprogressRecoveryEmptyFile() throws IOException {
    File f = new File(System.getProperty("test.build.data", "build/test/data") + "/filejournaltest1");
    StorageDirectory sd = setupEdits(f, 10, 5);

    File[] files = new File(f, "current").listFiles(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          if (name.startsWith("edits_inprogress")) {
            return true;
          }
          return false;
        }
      });
    assertEquals(files.length, 1);
    files[0].delete();
    files[0].createNewFile();
   
    FileJournalManager jm = new FileJournalManager(sd);
    assertEquals(15, jm.getNumberOfTransactions(1));
    }*/
}
