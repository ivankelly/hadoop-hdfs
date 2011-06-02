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

import java.util.concurrent.AtomicInteger;
import java.util.concurrent.CountDownLatch;

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.AddCallback;

import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DataOutputBuffer;
import java.io.IOException;

class BookKeeperEditLogOutputStream extends EditLogOutputStream implements AddCallback {
  private final DataOutputBuffer bufCurrent;
  private final AtomicInteger outstandingRequests;
  private static final int TRANSMISSION_THRESHOLD = 1024;
  private final LedgerHandle lh;
  private CountDownLatch syncLatch;
  private final WriteLock wl;

  protected BookKeeperEditLogOutputStream(LedgerHandle lh, WriteLock wl) throws IOException {
    super();
    
    bufCurrent = new DataOutputBuffer();
    outstandingRequests = new AtomicInteger(0);;
    syncLatch = null;
    this.lh = lh;
    this.wl = wl;
    this.wl.acquire();
  }

  @Override
  public void create() throws IOException {
    // noop
  }

  @Override
  public void close() throws IOException {
    setReadyToFlush();
    flushAndSync();
    lh.close();
    wl.release();
  }

  @Override
  public void abort() throws IOException {
    lh.close();
    wl.release();
  }

  @Override
  public void write(byte op, long txid, Writable ... writables) throws IOException {
    checkWriteLock();

    int start = bufCurrent.getLength();
    bufCurrent.write(op);
    bufCurrent.writeLong(txid);
    for (Writable w : writables) {
      w.write(bufCurrent);
    }
    // write transaction checksum
    int end = bufCurrent.getLength();
    Checksum checksum = FSEditLog.getChecksum();
    checksum.reset();
    checksum.update(bufCurrent.getData(), start, end-start);
    int sum = (int)checksum.getValue();
    bufCurrent.writeInt(sum);
    
    if (bufCurrent.length() > TRANSMISSION_THRESHOLD) {
      transmit();
    }
  }

  @Override
  public void setReadyToFlush() throws IOException {
    checkWriteLock();

    transmit();
    
    synchronized(outstandingRequests) {
      syncLatch = new CountDownLatch(outstandingRequests.get());
    }
  }

  @Override
  public void flushAndSync() throws IOException {
    checkWriteLock();
        
    assert(syncLatch != null);
    syncLatch.await();
    syncLatch = null;
    // wait for whatever we wait on
  }
  
  /**
   * Transmit the current buffer to bookkeeper.
   * Synchronised at the FSEditLog level. #write() and #setReadyToFlush() 
   * are never called at the same time.
   */
  private void transmit() throws IOException {
    checkWriteLock();

    lh.asyncAddEntry(bufCurrent.getData(), this, null);
    bufCurrent.reset();
    outstandingRequests.incrementAndGet();
  }

  public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
    synchronized(outstandingRequests) {
      outstandingRequests.decrementAndGet();
      CountDownLatch l = syncLatch;
      if (l != null) {
        l.countDown();
      }
    }
  }

  public void checkWriteLock() throws IOException {
    if (!wl.haveLock()) {
      throw new IOException("Lost writer lock");
    }
  }

  @Override
  public long length() throws IOException {
    return lh.getLength();
  }

  @Override
  public String getName() {
    return "IKFIXME";
  }

  @Override
  public JournalType getType() {
    assert (false);
    return null;
  }
}
