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

import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.conf.Configuration;

import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

import java.util.Enumeration;
import java.io.IOException;
import java.io.BufferedInputStream;
import java.io.DataInputStream;

import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BookKeeperJournalManager implements JournalManager {
  static final Log LOG = LogFactory.getLog(BookKeeperJournalManager.class);

  public static final String BKJM_ZOOKEEPER_QUORUM = "dfs.namenode.bookkeeperjournal.zkquorum";
  public static final String BKJM_ZOOKEEPER_PATH = "dfs.namenode.bookkeeperjournal.zkpath";
  public static final String BKJM_ZOOKEEPER_PATH_DEFAULT = "/hdfsjournal";
  public static final String BKJM_BOOKKEEPER_ENSEMBLE_SIZE = "dfs.namenode.bookkeeperjournal.ensembleSize";
  public static final String BKJM_BOOKKEEPER_QUORUM_SIZE = "dfs.namenode.bookkeeperjournal.quorumSize";
  public static final int BKJM_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT = 3;
  public static final int BKJM_BOOKKEEPER_QUORUM_SIZE_DEFAULT = 2;
  public static final String BKJM_BOOKKEEPER_DIGEST_PW = "dfs.namenode.bookkeeperjournal.digestpw";
  public static final String BKJM_BOOKKEEPER_DIGEST_PW_DEFAULT = "";

  private static final int BKJM_LAYOUT_VERSION = -1;

  private final ZooKeeper zkc;
  private final BookKeeper bkc;
  private final WriteLock fence;
  private final String ledgerpath;
  private final int ensembleSize;
  private final int quorumSize;
  private final String digestpw;
  
  private LedgerHandle currentLedger = null;

  private int bytesToInt(byte[] b) {
    assert b.length >= 4;
    return b[0] << 24 | b[1] << 16 | b[2] << 8 | b[3];
  }

  private byte[] intToBytes(int i) {
    return new byte[] {
      (byte)(i >> 24),
      (byte)(i >> 16),
      (byte)(i >> 8),
      (byte)(i) };
  }
  public BookKeeperJournalManager(URI uri, Configuration conf) throws IOException {
    String zkconnect = conf.get(BKJM_ZOOKEEPER_QUORUM);
    String zkpath = conf.get(BKJM_ZOOKEEPER_PATH, BKJM_ZOOKEEPER_PATH_DEFAULT);
    ensembleSize = conf.getInt(BKJM_BOOKKEEPER_ENSEMBLE_SIZE, BKJM_BOOKKEEPER_ENSEMBLE_SIZE_DEFAULT);
    quorumSize = conf.getInt(BKJM_BOOKKEEPER_QUORUM_SIZE, BKJM_BOOKKEEPER_QUORUM_SIZE_DEFAULT);
    
    ledgerpath = zkpath + "/ledgers";
    String lockpath = zkpath + "/lock";
    String versionpath = zkpath + "/version";
    digestpw = conf.get(BKJM_BOOKKEEPER_DIGEST_PW, BKJM_BOOKKEEPER_DIGEST_PW_DEFAULT);

    if (zkconnect == null) {
      throw new IOException(BKJM_ZOOKEEPER_QUORUM + " not set in configuration");
    }
    try {
      zkc = new ZooKeeper(zkconnect, 3000, null);

      if (zkc.exists(zkpath, false) == null) {
        zkc.create(zkpath, new byte[] {'0'}, 
            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }

      Stat versionstat = zkc.exists(versionpath, false);
      if (versionstat != null) {
        byte[] d = zkc.getData(versionpath, false, versionstat);
        // There's only one version at the moment
        assert bytesToInt(d) == BKJM_LAYOUT_VERSION;
      } else {
        zkc.create(versionpath, intToBytes(BKJM_LAYOUT_VERSION),
                   Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }

      if (zkc.exists(ledgerpath, false) == null) {
        zkc.create(ledgerpath, new byte[] {'0'}, 
            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }

      bkc = new BookKeeper(zkc);
    } catch (Exception e) {
      throw new IOException("Error initializing zk");
    }

    fence = new WriteLock(zkc, lockpath);

  }

  @Override
  public EditLogOutputStream startLogSegment(long txId) throws IOException {
    // Make sure im the writer, otherwise throw exception
    // check if ledgers/inprogress exists, if so throw
    // create ledger
    // create ledgers/inprogress <version>;<ledgerid>;<first>
    // create and retrun editlogoutputstream with this ledger
    recoverUnclosedStreams();
    
    fence.acquire();

    if (currentLedger != null) {
      throw new IOException("Already writing to a ledger, id="
                            + currentLedger.getId());
    }
    try {
      currentLedger = bkc.createLedger(ensembleSize, quorumSize, 
                                       BookKeeper.DigestType.MAC, 
                                       digestpw.getBytes());
      Ledger l = new Ledger(FSConstants.LAYOUT_VERSION, currentLedger.getId(), txId);
      l.write(zkc, inprogressZNode());

      return new BookKeeperEditLogOutputStream(currentLedger, fence);
    } catch (Exception e) {
      if (currentLedger != null) {
        try {
          currentLedger.close();
        } catch (Exception e2) {
          //log& ignore, an IOException will be thrown soon
          LOG.error("Error closing ledger", e2);
        }
      }
      throw new IOException("Error creating ledger", e);
    }
  }

  @Override
  public void finalizeLogSegment(long firstTxId, long lastTxId) throws IOException {
    // make sure im the write, otherwise throw
    // check if ledgers/inprogress exists, and make sure it has matching txid
    // create ledger/first-last with <version>;<ledgerid>;<first>;<last> as content.
    // delete inprogress
    fence.checkWriteLock();
    
    String inprogressPath = inprogressZNode();
    try {
      Ledger l = Ledger.read(zkc, inprogressPath);
      
      if (l.getStartTxId() != firstTxId) {
        throw new IOException("Transaction id not as expected, " 
            + l.getStartTxId() + " found, " + firstTxId + " expected");
      } else if (currentLedger != null
                 && l.getLedgerId() != currentLedger.getId()) {
        throw new IOException("Active ledger has different ID to inprogress. " 
                              + l.getLedgerId() + " found, "
                              + currentLedger.getId() + " expected");
      }

      l.finalizeLedger(lastTxId);
      String finalisedPath = finalizedLedgerZNode(firstTxId, lastTxId);
      try {
        l.write(zkc, finalisedPath);
      } catch (KeeperException.NodeExistsException nee) {
        if (!l.verify(zkc, finalisedPath)) {
          throw new IOException("Node " + finalisedPath + " but data doesn't match");
        }
      }
      zkc.delete(inprogressPath, -1);
    } catch (Exception e) {
      throw new IOException("Error finalising ledger", e);
    } finally {
      fence.release();
      
      try {
        currentLedger.close();
      } catch (Exception e2) {
        throw new IOException("Error closing ledger", e2);
      } finally {
        currentLedger = null;
      }
    }
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
    fence.acquire();
    try {
      Ledger l = Ledger.read(zkc, inprogressZNode());
      LedgerHandle lh = bkc.openLedger(l.getLedgerId(), BookKeeper.DigestType.MAC, 
                                       digestpw.getBytes());

      long lastAddConfirmed = lh.getLastAddConfirmed();
      Enumeration<LedgerEntry> entries = lh.readEntries(lastAddConfirmed, lastAddConfirmed);

      BufferedInputStream bin
        = new BufferedInputStream(entries.nextElement().getEntryInputStream());
      Checksum checksum = FSEditLog.getChecksum();
      DataInputStream in
        = new DataInputStream(new CheckedInputStream(bin, checksum));

      FSEditLogLoader loader = new FSEditLogLoader();
      
      FSEditLogOp op = FSEditLogOp.readOp(in, l.getVersion(), checksum);
      long endTxId = -1;
      while (op != null) {
        if (endTxId == -1 || op.getTxId() == endTxId+1) {
          endTxId = op.getTxId();
        }
        op = FSEditLogOp.readOp(in, l.getVersion(), checksum);
      }
      finalizeLogSegment(l.getStartTxId(), endTxId);
    } catch (KeeperException.NoNodeException nne) {
      // nothing to recover, ignore
    } catch (Exception e) {
      throw new IOException("Exception recovering inprogress ledger", e);
    } finally {
      if (fence.haveLock()) {
        fence.release();
      }
    }
  }

  /**
   * Set the amount of memory that this stream should use to buffer edits.
   */
  @Override
  public void setOutputBufferCapacity(int size) {
  }

  public void process(WatchedEvent event) {
    if (event.getState() == KeeperState.Disconnected
        || event.getState() == KeeperState.Expired) {
    } else {
    }
  }

  String finalizedLedgerZNode(long startTxId, long endTxId) {
    return String.format("%s/edits_%018d_%018d",
                         ledgerpath, endTxId, endTxId);
  }
    
  String inprogressZNode() {
    return ledgerpath + "/inprogress";
  }

  public static class Ledger {
    final long ledgerId;
    final int version;
    final long startTxId;
    long endTxId;
    boolean inprogress;

    Ledger(int version, long ledgerId, long startTxId) {
      this.ledgerId = ledgerId;
      this.version = version;
      this.startTxId = startTxId;
      this.endTxId = 0;
      this.inprogress = true;
    }

    Ledger(int version, long ledgerId, long startTxId, long endTxId) {
      this.ledgerId = ledgerId;
      this.version = version;
      this.startTxId = startTxId;
      this.endTxId = endTxId;
      this.inprogress = false;
    }

    long getStartTxId() {
      return startTxId;
    }

    long getLedgerId() {
      return ledgerId;
    }

    int getVersion() {
      return version;
    }

    void finalizeLedger(long endTxId) {
      assert this.endTxId == 0;
      this.endTxId = endTxId;
      this.inprogress = false;      
    }

    static Ledger read(ZooKeeper zkc, String path) throws IOException, KeeperException.NoNodeException  {
      try {
        byte[] data = zkc.getData(path, false, null);
        String[] parts = new String(data).split(";");
        if (parts.length == 3) {
          int version = Integer.valueOf(parts[0]);
          long ledgerId = Long.valueOf(parts[1]);
          long txId = Long.valueOf(parts[2]);
          return new Ledger(version, ledgerId, txId);
        } else if (parts.length == 4) {
          int version = Integer.valueOf(parts[0]);
          long ledgerId = Long.valueOf(parts[1]);
          long startTxId = Long.valueOf(parts[2]);
          long endTxId = Long.valueOf(parts[3]);
          return new Ledger(version, ledgerId, startTxId, endTxId);
        } else {
          throw new IOException("Invalid ledger entry, "
                                + new String(data));
        }
      } catch(KeeperException.NoNodeException nne) {
        throw nne;
      } catch(Exception e) {
        throw new IOException("Error reading from zookeeper", e);
      }
    }
    
    void write(ZooKeeper zkc, String path) throws IOException, KeeperException.NodeExistsException {
      String finalisedData;
      if (inprogress) {
        finalisedData = String.format("%d;%d;%d",
                                      version, ledgerId, startTxId);
      } else {
        finalisedData = String.format("%d;%d;%d;%d",
                                      version, ledgerId, startTxId, endTxId);
        
      }
      try {
        zkc.create(path, finalisedData.getBytes(), 
                   Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } catch (KeeperException.NodeExistsException nee) {
        throw nee;
      } catch (Exception e) {
        throw new IOException("Error creating ledger znode");
      } 
    }

    boolean verify(ZooKeeper zkc, String path) {
      try {
        Ledger other = Ledger.read(zkc, path);
        return other == this;
      } catch (Exception e) {
        LOG.error("Couldn't verify data in " + path, e);
        return false;
      }
    }

    public boolean equals(Object o) {
      if (!(o instanceof Ledger)) {
        return false;
      }
      Ledger ol = (Ledger)o;
      return ledgerId == ol.ledgerId
        && startTxId == ol.startTxId
        && endTxId == ol.endTxId
        && version == ol.version;
    }
  }
}
