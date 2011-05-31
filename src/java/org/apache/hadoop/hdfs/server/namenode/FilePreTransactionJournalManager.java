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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.io.File;
import java.io.IOException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.DataInputStream;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;

import com.google.common.annotations.VisibleForTesting;

/**
 * JournalManager for loading legacy (Pre-1073)  edit logs.
 * This journal manager can't be used for output.
 */
public class FilePreTransactionJournalManager implements JournalManager {
  private static final Log LOG
    = LogFactory.getLog(FilePreTransactionJournalManager.class);

  private final StorageDirectory sd;
  private final List<File> files;

  private enum State { SERVING_NONE, SERVING_EDITS, SERVING_EDITS_NEW };
  private State state;

  public FilePreTransactionJournalManager(StorageDirectory sd,
                                          List<File> files) {
    this.sd = sd;
    this.state = State.SERVING_NONE;
    this.files = files;
  }

  @Override
  public EditLogOutputStream startLogSegment(long txid) throws IOException {
    throw new IOException("Cannot output with "
                          + "FilePreTransactionJournalManager");
  }

  @Override
  public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    throw new IOException("Cannot output with "
                          + "FilePreTransactionJournalManager");
  }

  @VisibleForTesting
  public StorageDirectory getStorageDirectory() {
    return sd;
  }

  @Override
  public String toString() {
    return "FileJournalManager for storage directory " + sd;
  }

  @Override
  public void setOutputBufferCapacity(int size) {
    // noop
  }

  @Override
  public void recoverUnclosedStreams() throws IOException {
  }


  @Override
  synchronized public EditLogInputStream getInputStream(long sinceTxnId)
      throws IOException {
    if (files.size() == 0) {
      throw new IOException("No edits files in " + sd);
    }
    if (state == State.SERVING_NONE) {
      state = State.SERVING_EDITS;
      return new EditLogFileInputStream(files.get(0));
    } else if (state == State.SERVING_EDITS && files.size() == 2) {
      state = State.SERVING_EDITS_NEW;
      return new EditLogFileInputStream(files.get(1));
    } else {
      throw new IOException("Invalid state. In state " + state + " and there "
                            + " are " + files.size() + "files");
    }
  }

  @Override
  public long getNumberOfTransactions(long sinceTxnId) throws IOException {
    long count = 0;
    for (File f : files) {
      BufferedInputStream bin = new BufferedInputStream(new FileInputStream(f));
      DataInputStream in = new DataInputStream(bin);

      FSEditLogLoader loader = new FSEditLogLoader();
      try {
        int logVersion = loader.readLogVersion(in);
        Checksum checksum = null;
        if (logVersion <= -28) { // support fsedits checksum
          checksum = FSEditLog.getChecksum();
          in = new DataInputStream(new CheckedInputStream(bin, checksum));
        }

        while (true) {
          FSEditLogOp op = FSEditLogOp.readOp(in, logVersion, checksum);
          count++;
        }
      } catch (IOException ioe) {
        // end of file found
        LOG.info("Found end of log", ioe);
      } finally {
        in.close();
      }
    }
    return count;
  }

}
