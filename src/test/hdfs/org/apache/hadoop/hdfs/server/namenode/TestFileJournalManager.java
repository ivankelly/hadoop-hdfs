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
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

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
      editlog.logSync();
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
  public void testInprogressRecovery() throws IOException {
    File f = new File(System.getProperty("test.build.data", "build/test/data") + "/filejournaltest0");
    StorageDirectory sd = setupEdits(f, 10, 5);

    FileJournalManager jm = new FileJournalManager(sd);
    assertEquals(17, jm.getNumberOfTransactions(1));
  }

  private long readAll(EditLogInputStream elis, long nextTxId) throws IOException {
    long count = 0L;
    
    Checksum checksum = FSEditLog.getChecksum();
    FSEditLogLoader loader = new FSEditLogLoader();
    BufferedInputStream bin = new BufferedInputStream(elis);
    DataInputStream in = new DataInputStream(new CheckedInputStream(bin, checksum));

    int logVersion = loader.readLogVersion(in);
    try {
      FSEditLogOp.Reader reader = new FSEditLogOp.Reader(in, logVersion, checksum);
      while (true) {
        FSEditLogOp op = reader.readOp();
        assertEquals(nextTxId, op.txid);
        nextTxId++;        
        count++;
      } 
    } catch (IOException ioe) {
      // no more to read
    } finally {
      in.close();
    }
    return count;
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
    File f = new File(System.getProperty("test.build.data", "build/test/data") + "/filejournaltest0");
    StorageDirectory sd = setupEdits(f, 20, 10);
    
    FileJournalManager jm = new FileJournalManager(sd);
    assertEquals(32, jm.getNumberOfTransactions(1));
    
    long startingTxId = 10;
    long numTransactionsToLoad = jm.getNumberOfTransactions(startingTxId);
    long numLoaded = 0;
    while (numLoaded < numTransactionsToLoad) {
      EditLogInputStream editIn = jm.getInputStream(startingTxId);
      long count = readAll(editIn, startingTxId);

      editIn.close();
      startingTxId += count;
      numLoaded += count;
    }

    assertEquals(23, numLoaded); // txns [10,...,32]
  }

  /**
   * You cannot make a request with a start transaction id which doesn't
   * match the start ID of some log segment. 
   */
  @Test(expected=IOException.class)
  public void testAskForTransactionsMidfile() throws IOException {
    File f = new File(System.getProperty("test.build.data", "build/test/data") + "/filejournaltest0");
    StorageDirectory sd = setupEdits(f, 20, 100);
    
    FileJournalManager jm = new FileJournalManager(sd);
    jm.getNumberOfTransactions(2);    
  }

  @Test
  public void testManyLogsWithGaps() throws IOException {
    File f = new File(System.getProperty("test.build.data", "build/test/data") + "/filejournaltest1");
    StorageDirectory sd = setupEdits(f, 10, 20);
    
    File[] files = new File(f, "current").listFiles(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          if (name.startsWith("edits_7-9")) {
            return true;
          }
          return false;
        }
      });
    assertEquals(files.length, 1);
    assertTrue(files[0].delete());
    
    FileJournalManager jm = new FileJournalManager(sd);
    assertEquals(6, jm.getNumberOfTransactions(1)); // txns [1,...,6] 
    // 7-9 have been deleted
    assertEquals(23, jm.getNumberOfTransactions(10)); // txns [10,...,32]
  }

  @Test
  public void testManyLogsWithGapWhereInprogressInMiddle() throws IOException {
    File f = new File(System.getProperty("test.build.data", "build/test/data") + "/filejournaltest1");
    StorageDirectory sd = setupEdits(f, 10, 20);
    
    File[] files = new File(f, "current").listFiles(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          if (name.startsWith("edits_7-9")) {
            return true;
          }
          return false;
        }
      });
    assertEquals(files.length, 1);
   
    String[] parts = files[0].getName().split("_")[1].split("-");
    File newfile = new File(files[0].getParent(), "edits_inprogress_" + parts[0]);
    assertTrue(files[0].renameTo(newfile));
    corruptAfterStartSegment(newfile);

    FileJournalManager jm = new FileJournalManager(sd);
    assertEquals(7, jm.getNumberOfTransactions(1)); // txns [1,...,7] 
    // 8 & 9 are corrupt
    assertEquals(23, jm.getNumberOfTransactions(10)); // txns [10,...,32]
  }


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