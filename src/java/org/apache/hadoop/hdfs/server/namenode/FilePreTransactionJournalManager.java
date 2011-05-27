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

import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * JournalManager for loading legacy (Pre-1073)  edit logs. 
 * This journal manager can't be used for output.
 */
public class FilePreTransactionJournalManager implements JournalManager {
  private static final Log LOG = LogFactory.getLog(FilePreTransactionJournalManager.class);

  private final StorageDirectory sd;

  public FilePreTransactionJournalManager(StorageDirectory sd, List<File> files) {
    this.sd = sd;
  }

  @Override
  public EditLogOutputStream startLogSegment(long txid) throws IOException {    
    throw new IOException("Cannot output with FilePreTransactionJournalManager");
  }

  @Override
  public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    throw new IOException("Cannot output with FilePreTransactionJournalManager");
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
  public EditLogInputStream getInputStream(long sinceTxnId) throws IOException {
    return null; // IKTODO
  }

  @Override
  public long getNumberOfTransactions(long sinceTxnId) throws IOException {
    return 0; // IKTODO
  }

}
