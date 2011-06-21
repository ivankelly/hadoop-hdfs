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
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

public class TestFileJournalManager {
  class AbortSpec {
    final int roll;
    final int logindex;

    AbortSpec(int roll, int logindex) {
      this.roll = roll;
      this.logindex = logindex;
    }
  }

  public static NNStorage setupEdits(List<URI> editUris, int numrolls, AbortSpec... abortAtRolls) 
      throws IOException {
    List<AbortSpec> aborts = new ArrayList<AbortSpec>(Arrays.asList(abortAtRolls));
    NNStorage storage = new NNStorage(new Configuration(),
                                      Collections.<URI>emptyList(),
                                      editUris);
    for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.EDITS)) {
      storage.format(sd);
    }
    FSEditLog editlog = new FSEditLog(storage);    
    editlog.open();
    editlog.logGenerationStamp((long)0);
    editlog.logSync();

    for (int i = 0; i < numrolls; i++) {
      editlog.rollEditLog();
      
      editlog.logGenerationStamp((long)i);
      editlog.logSync();

      if (aborts.size() > 0 
          && aborts.get(0).roll == i) {
        AbortSpec spec = aborts.remove(0);
        editlog.getJournals().get(spec.logindex).abort();
      } 
      
      editlog.logGenerationStamp((long)i);
      editlog.logSync();
    }
    editlog.close();

    return storage;
  }

  @Test
  public void testInprogressRecovery() throws IOException {
    File f = new File(System.getProperty("test.build.data", "build/test/data") + "/filejournaltest0");
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()), 5, new AbortSpec(4, 0));
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();

    FileJournalManager jm = new FileJournalManager(sd);
    assertEquals(18, jm.getNumberOfTransactions(1));
  }

  private long readAll(EditLogInputStream elis, long nextTxId) throws IOException {
    long count = 0L;
    
    Checksum checksum = FSEditLog.getChecksum();
    FSEditLogLoader loader = new FSEditLogLoader();
    BufferedInputStream bin = new BufferedInputStream(elis);
    DataInputStream in = new DataInputStream(new CheckedInputStream(bin, checksum));

    
    int logVersion = loader.readLogVersion(in);
    try {
      FSEditLogOp.Reader reader = new FSEditLogOp.Reader(in, logVersion,
                                                         checksum);
      FSEditLogOp op;
      while ((op = reader.readOp()) != null) {
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
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()), 10, new AbortSpec(10, 0));
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();

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
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()), 10);
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();
    
    FileJournalManager jm = new FileJournalManager(sd);
    jm.getNumberOfTransactions(2);    
  }

  @Test
  public void testManyLogsWithGaps() throws IOException {
    File f = new File(System.getProperty("test.build.data", "build/test/data") + "/filejournaltest1");
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()), 10);
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();

    File[] files = new File(f, "current").listFiles(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          if (name.startsWith(NNStorage.getFinalizedEditsFileName(7,9))) {
            return true;
          }
          return false;
        }
      });
    assertEquals(files.length, 1);
    assertTrue(files[0].delete());
    
    FileJournalManager jm = new FileJournalManager(sd);
    try {
      jm.getNumberOfTransactions(1);
    } catch (IOException ioe) {
      assertTrue(ioe.getMessage().matches("^Gap in transactions 7 - 9$"));
    }
    // 7-9 have been deleted
    assertEquals(23, jm.getNumberOfTransactions(10)); // txns [10,...,32]
  }

  @Test
  public void testManyLogsWithGapWhereInprogressInMiddle() throws IOException {
    File f = new File(System.getProperty("test.build.data", "build/test/data") + "/filejournaltest1");
    NNStorage storage = setupEdits(Collections.<URI>singletonList(f.toURI()), 10, new AbortSpec(4, 0));
    StorageDirectory sd = storage.dirIterator(NameNodeDirType.EDITS).next();
    
    FileJournalManager jm = new FileJournalManager(sd);
    try {
      jm.getNumberOfTransactions(1); // txns [1,...,7] 
    } catch (IOException ioe) {
      assertTrue(ioe.getMessage().matches("^Gap in transactions 7 - 9$"));
    }
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
