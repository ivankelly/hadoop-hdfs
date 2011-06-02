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

import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;

public class BookKeeperJournalManager implements JournalManager {
  public static final String BKJM_ZOOKEEPER_QUORUM = "dfs.namenode.bookkeeperjournal.zkquorum";

  public BookKeeperJournalManager(URI uri, Configuration conf) throws IOException {
    // expect an empty ZK znode. if it doesn't exist, create it 
    // if creation fails, throw
    // create znode for ledgers
    // create znode for fencing
  }

  @Override
  public EditLogOutputStream startLogSegment(long txId) throws IOException {
    // Make sure im the writer, otherwise throw exception
    // check if ledgers/inprogress exists, if so throw
    // create ledger
    // create ledgers/inprogress <version>;<ledgerid>;<first>
    // create and retrun editlogoutputstream with this ledger
    return null;
  }

  @Override
  public void finalizeLogSegment(long firstTxId, long lastTxId) throws IOException {
    // make sure im the write, otherwise throw
    // check if ledgers/inprogress exists, and make sure it has matching txid
    // create ledger/first-last with <version>;<ledgerid>;<first>;<last> as content.
    // delete inprogress
  }

  @Override
  public EditLogInputStream getInputStream(long fromTxnId) throws IOException {
    // find ledger with start id of fromTxnId
    // create inputstream from this
    return null;
  }

  @Override
  public long getNumberOfTransactions(long fromTxnId) throws IOException {
    return 0L;
  }

  @Override
  public void recoverUnclosedStreams() throws IOException {
    // check that write lock doesn't exist. take it.
    // read ledgers/inprogress
    // open ledger, read last entry
    // read transactions from last entry
    // if ledgers/first-last exists, verify contents, 
    // if ledgers/first-<somethingelse> throw
    // else create ledgers/first-last
    // delete inprogress

    // release write lock.
  }

  /**
   * Set the amount of memory that this stream should use to buffer edits.
   */
  @Override
  public void setOutputBufferCapacity(int size) {
  }
}
