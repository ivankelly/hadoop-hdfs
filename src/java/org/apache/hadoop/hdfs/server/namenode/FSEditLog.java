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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.net.URI;
import java.net.URISyntaxException;
import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DeprecatedUTF8;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import static org.apache.hadoop.hdfs.server.common.Util.now;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NNStorageListener;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.token.delegation.DelegationKey;

import com.google.common.base.Preconditions;

import static org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes.*;

/**
 * FSEditLog maintains a log of the namespace modifications.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSEditLog implements NNStorageListener {
  
  static final String NO_JOURNAL_STREAMS_WARNING = "!!! WARNING !!!" +
      " File system changes are not persistent. No journal streams.";

  static final Log LOG = LogFactory.getLog(FSEditLog.class);

  private volatile int sizeOutputFlushBuffer = 512*1024;

  private enum State {
    UNINITIALIZED,
    BETWEEN_LOG_SEGMENTS,
    IN_SEGMENT,
    CLOSED;
  }
  
  private State state = State.UNINITIALIZED;

  private ArrayList<EditLogOutputStream> editStreams = new ArrayList<EditLogOutputStream>();
  
  // the txid of the last edit log roll - ie if this value is
  // N, we are currently writing to edits_inprogress_N
  private long lastRollTxId = -1;

  // a monotonically increasing counter that represents transactionIds.
  private long txid = 0;

  // stores the last synced transactionId.
  private long synctxid = 0;
  
  // the time of printing the statistics to the log file.
  private long lastPrintTime;

  // is a sync currently running?
  private volatile boolean isSyncRunning;

  // is an automatic sync scheduled?
  private volatile boolean isAutoSyncScheduled = false;

  // these are statistics counters.
  private long numTransactions;        // number of transactions
  private long numTransactionsBatchedInSync;
  private long totalTimeTransactions;  // total time for all transactions
  private NameNodeMetrics metrics;

  private Configuration conf;
  private NNStorage storage;

  private static class TransactionId {
    public long txid;

    TransactionId(long value) {
      this.txid = value;
    }
  }

  // stores the most current transactionId of this thread.
  private static final ThreadLocal<TransactionId> myTransactionId = new ThreadLocal<TransactionId>() {
    protected synchronized TransactionId initialValue() {
      return new TransactionId(Long.MAX_VALUE);
    }
  };

  FSEditLog(Configuration conf, NNStorage storage) {
    this.conf = conf;
    isSyncRunning = false;
    this.storage = storage;
    this.storage.registerListener(this);
    metrics = NameNode.getNameNodeMetrics();
    lastPrintTime = now();
  }

  /**
   * Initialize the list of edit journals
   */
  private void initJournals() throws IOException {
    assert editStreams.isEmpty();

    Preconditions.checkState(state == State.UNINITIALIZED || state == State.CLOSED,
        "Bad state: %s", state);

    ArrayList<StorageDirectory> al = null;
    for (Iterator<StorageDirectory> it 
         = storage.dirIterator(NameNodeDirType.EDITS); it.hasNext();) {
      StorageDirectory sd = it.next();
      try {
        JournalFactory factory = new FileJournalFactory(conf, sd.getRoot().toURI(), 
                                                        storage, sd);
        editStreams.add(factory.getOutputStream());
      } catch (IOException e) {
        LOG.warn("Unable to open edit log file in " + sd);
        // Remove the directory from list of storage directories
        if(al == null) al = new ArrayList<StorageDirectory>(1);
        al.add(sd);
      }
    }    
    if(al != null) storage.reportErrorsOnDirectories(al);
    
    if (editStreams.isEmpty()) {
      LOG.error("No edits directories configured!");
    }
    
    state = State.BETWEEN_LOG_SEGMENTS;
  }
 
  private int getNumEditsDirs() {
   return storage.getNumStorageDirs(NameNodeDirType.EDITS);
  }

  synchronized int getNumEditStreams() {
    return editStreams == null ? 0 : editStreams.size();
  }

  /**
   * Return the currently active edit streams.
   * This should be used only by unit tests.
   */
  ArrayList<EditLogOutputStream> getEditStreams() {
    return editStreams;
  }

  /**
   * Create empty edit log files.
   * Initialize the output stream for logging.
   * 
   * @throws IOException
   */
  synchronized void open() throws IOException {
    if (state == State.UNINITIALIZED
	|| state == State.CLOSED) {
      initJournals();
    }
    
    startLogSegment(getLastWrittenTxId() + 1);
  }
  
  synchronized boolean isOpen() {
    return state == State.BETWEEN_LOG_SEGMENTS ||
           state == State.IN_SEGMENT;
    // TODO maybe we just want IN_SEGMENT here?
    // Look at all uses of this function to clarify its meaning.
  }

  synchronized void createEditLogFile(File name) throws IOException {
    waitForSyncToFinish();
    
    /*EditLogOutputStream eStream = new EditLogFileOutputStream(null, 
	        sizeOutputFlushBuffer);
    eStream.create();
    eStream.close();*/
  }
  
  /**
   * Shutdown the file store.
   */
  synchronized void close() {
    if (state == State.CLOSED) {
      LOG.warn("Closing log when already closed", new Exception());
      return;
    }

    if (state == State.IN_SEGMENT) {
      waitForSyncToFinish();
      if (editStreams.isEmpty()) {
        // TODO when would this be the case?
        return;
      }
      endCurrentLogSegment();

      mapStreamsAndReportErrors(new StreamClosure() {
	  @Override
	  public void apply(EditLogOutputStream stream) throws IOException {
	    stream.close();
	  }      
	}, "Closing stream");
    }
    editStreams.clear();

    state = State.CLOSED;
  }

  /**
   * Close and remove edit log stream.
   * @param index of the stream
   */
  synchronized private void removeStream(int index) {
    EditLogOutputStream eStream = editStreams.get(index);
    try {
      eStream.close();
    } catch (Exception e) {}
    editStreams.remove(index);
  }

  /**
   * The specified streams have IO errors. Close and remove them.
   */
  synchronized
  void disableAndReportErrorOnStreams(List<EditLogOutputStream> errorStreams) {
    if (errorStreams == null || errorStreams.size() == 0) {
      return;                       // nothing to do
    }
    ArrayList<StorageDirectory> errorDirs = new ArrayList<StorageDirectory>();
    for (EditLogOutputStream e : errorStreams) {
      if (e.getType() == JournalType.FILE) {
        errorDirs.add(getStorageDirectoryForStream(e));
      } else {
        disableStream(e);
      }
    }

    try {
      storage.reportErrorsOnDirectories(errorDirs);
    } catch (IOException ioe) {
      LOG.error("Problem erroring streams " + ioe);
    }
  }


  /**
   * get an editStream corresponding to a sd
   * @param es - stream to remove
   * @return the matching stream
   */
  StorageDirectory getStorage(EditLogOutputStream es) {
    String parentStorageDir = ((EditLogFileOutputStream)es).getFile()
    .getParentFile().getParentFile().getAbsolutePath();

    Iterator<StorageDirectory> it = storage.dirIterator(); 
    while (it.hasNext()) {
      StorageDirectory sd = it.next();
      LOG.info("comparing: " + parentStorageDir + " and " + sd.getRoot().getAbsolutePath()); 
      if (parentStorageDir.equals(sd.getRoot().getAbsolutePath()))
        return sd;
    }
    return null;
  }
  
  /**
   * get an editStream corresponding to a sd
   * @param sd
   * @return the matching stream
   */
  synchronized EditLogOutputStream getEditsStream(StorageDirectory sd) {
    for (EditLogOutputStream es : editStreams) {
      File parentStorageDir = ((EditLogFileOutputStream)es).getFile()
        .getParentFile().getParentFile();
      if (parentStorageDir.getName().equals(sd.getRoot().getName()))
        return es;
    }
    return null;
  }

  /**
   * Write an operation to the edit log. Do not sync to persistent
   * store yet.
   */

  void logEdit(final FSEditLogOpCodes opCode, final Writable ... writables) {
    assert state != State.UNINITIALIZED && state != State.CLOSED;

    synchronized (this) {
      // wait if an automatic sync is scheduled
      waitIfAutoSyncScheduled();
      
      if(getNumEditStreams() == 0)
        throw new java.lang.IllegalStateException(NO_JOURNAL_STREAMS_WARNING);

      // Only start a new transaction for OPs which will be persisted to disk.
      // Obviously this excludes control op codes.
      long start = now();
      if (opCode.getOpCode() < FSEditLogOpCodes.OP_JSPOOL_START.getOpCode()) {
        start = beginTransaction();
      }

      mapStreamsAndReportErrors(new StreamClosure() {
          @Override
          public void apply(EditLogOutputStream stream) throws IOException {
            if(!stream.isOperationSupported(opCode.getOpCode()))
              return;
            stream.write(opCode.getOpCode(), txid, writables);
          }      
        }, "Writing op to stream");

      endTransaction(start);
      
      // check if it is time to schedule an automatic sync
      if (!shouldForceSync()) {
        return;
      }
      isAutoSyncScheduled = true;
    }
    
    // sync buffered edit log entries to persistent store
    logSync();
  }

  /**
   * Wait if an automatic sync is scheduled
   * @throws InterruptedException
   */
  synchronized void waitIfAutoSyncScheduled() {
    try {
      while (isAutoSyncScheduled) {
        this.wait(1000);
      }
    } catch (InterruptedException e) {
    }
  }
  
  /**
   * Signal that an automatic sync scheduling is done if it is scheduled
   */
  synchronized void doneWithAutoSyncScheduling() {
    if (isAutoSyncScheduled) {
      isAutoSyncScheduled = false;
      notifyAll();
    }
  }
  
  /**
   * Check if should automatically sync buffered edits to 
   * persistent store
   * 
   * @return true if any of the edit stream says that it should sync
   */
  private boolean shouldForceSync() {
    for (EditLogOutputStream eStream : editStreams) {
      if (eStream.shouldForceSync()) {
        return true;
      }
    }
    return false;
  }
  
  private long beginTransaction() {
    assert Thread.holdsLock(this);
    // get a new transactionId
    txid++;

    //
    // record the transactionId when new data was written to the edits log
    //
    TransactionId id = myTransactionId.get();
    id.txid = txid;
    return now();
  }
  
  private void endTransaction(long start) {
    assert Thread.holdsLock(this);
    
    // update statistics
    long end = now();
    numTransactions++;
    totalTimeTransactions += (end-start);
    if (metrics != null) // Metrics is non-null only when used inside name node
      metrics.transactions.inc((end-start));
  }

  /**
   * Return the transaction ID of the last transaction written to the log.
   */
  synchronized long getLastWrittenTxId() {
    return txid;
  }
  
  /**
   * @return the last transaction ID written to "edits" before rolling to
   * edits_new
   */
  synchronized long getLastRollTxId() {
    return lastRollTxId;
  }
  
  /**
   * Set the transaction ID to use for the next transaction written.
   */
  synchronized void setNextTxId(long nextTxId) {
    assert synctxid <= txid &&
       nextTxId >= txid : "May not decrease txid." +
      " synctxid=" + synctxid +
      " txid=" + txid +
      " nextTxid=" + nextTxId;
    txid = nextTxId - 1;
  }
  
  /**
   * Blocks until all ongoing edits have been synced to disk.
   * This differs from logSync in that it waits for edits that have been
   * written by other threads, not just edits from the calling thread.
   *
   * NOTE: this should be done while holding the FSNamesystem lock, or
   * else more operations can start writing while this is in progress.
   */
  void logSyncAll() throws IOException {
    // Record the most recent transaction ID as our own id
    synchronized (this) {
      TransactionId id = myTransactionId.get();
      id.txid = txid;
    }
    // Then make sure we're synced up to this point
    logSync();
  }
  
  /**
   * Sync all modifications done by this thread.
   *
   * The internal concurrency design of this class is as follows:
   *   - Log items are written synchronized into an in-memory buffer,
   *     and each assigned a transaction ID.
   *   - When a thread (client) would like to sync all of its edits, logSync()
   *     uses a ThreadLocal transaction ID to determine what edit number must
   *     be synced to.
   *   - The isSyncRunning volatile boolean tracks whether a sync is currently
   *     under progress.
   *
   * The data is double-buffered within each edit log implementation so that
   * in-memory writing can occur in parallel with the on-disk writing.
   *
   * Each sync occurs in three steps:
   *   1. synchronized, it swaps the double buffer and sets the isSyncRunning
   *      flag.
   *   2. unsynchronized, it flushes the data to storage
   *   3. synchronized, it resets the flag and notifies anyone waiting on the
   *      sync.
   *
   * The lack of synchronization on step 2 allows other threads to continue
   * to write into the memory buffer while the sync is in progress.
   * Because this step is unsynchronized, actions that need to avoid
   * concurrency with sync() should be synchronized and also call
   * waitForSyncToFinish() before assuming they are running alone.
   */
  public void logSync() {
    ArrayList<EditLogOutputStream> errorStreams = null;
    long syncStart = 0;

    // Fetch the transactionId of this thread. 
    long mytxid = myTransactionId.get().txid;
    ArrayList<EditLogOutputStream> streams = new ArrayList<EditLogOutputStream>();
    boolean sync = false;
    try {
      synchronized (this) {
        try {
        printStatistics(false);
  
        // if somebody is already syncing, then wait
        while (mytxid > synctxid && isSyncRunning) {
          try {
            wait(1000);
          } catch (InterruptedException ie) { 
          }
        }
  
        //
        // If this transaction was already flushed, then nothing to do
        //
        if (mytxid <= synctxid) {
          numTransactionsBatchedInSync++;
          if (metrics != null) // Metrics is non-null only when used inside name node
            metrics.transactionsBatchedInSync.inc();
          return;
        }
     
        // now, this thread will do the sync
        syncStart = txid;
        isSyncRunning = true;
        sync = true;
  
        // swap buffers
        assert editStreams.size() > 0 : "no editlog streams";
        for(EditLogOutputStream eStream : editStreams) {
          try {
            eStream.setReadyToFlush();
            streams.add(eStream);
          } catch (IOException ie) {
            LOG.error("Unable to get ready to flush.", ie);
            //
            // remember the streams that encountered an error.
            //
            if (errorStreams == null) {
              errorStreams = new ArrayList<EditLogOutputStream>(1);
            }
            errorStreams.add(eStream);
          }
        }
        } finally {
          // Prevent RuntimeException from blocking other log edit write 
          doneWithAutoSyncScheduling();
        }
      }
  
      // do the sync
      long start = now();
      for (EditLogOutputStream eStream : streams) {
        try {
          eStream.flush();
        } catch (IOException ie) {
          LOG.error("Unable to sync edit log.", ie);
          //
          // remember the streams that encountered an error.
          //
          if (errorStreams == null) {
            errorStreams = new ArrayList<EditLogOutputStream>(1);
          }
          errorStreams.add(eStream);
        }
      }
      long elapsed = now() - start;
      disableAndReportErrorOnStreams(errorStreams);
  
      if (metrics != null) // Metrics non-null only when used inside name node
        metrics.syncs.inc(elapsed);
    } finally {
      // Prevent RuntimeException from blocking other log edit sync 
      synchronized (this) {
        if (sync) {
          synctxid = syncStart;
          isSyncRunning = false;
        }
        this.notifyAll();
     }
    }
  }

  //
  // print statistics every 1 minute.
  //
  private void printStatistics(boolean force) {
    long now = now();
    if (lastPrintTime + 60000 > now && !force) {
      return;
    }
    if (editStreams == null || editStreams.size()==0) {
      return;
    }
    lastPrintTime = now;
    StringBuilder buf = new StringBuilder();
    buf.append("Number of transactions: ");
    buf.append(numTransactions);
    buf.append(" Total time for transactions(ms): ");
    buf.append(totalTimeTransactions);
    buf.append("Number of transactions batched in Syncs: ");
    buf.append(numTransactionsBatchedInSync);
    buf.append(" Number of syncs: ");
    buf.append(editStreams.get(0).getNumSync());
    buf.append(" SyncTimes(ms): ");

    int numEditStreams = editStreams.size();
    for (int idx = 0; idx < numEditStreams; idx++) {
      EditLogOutputStream eStream = editStreams.get(idx);
      buf.append(eStream.getTotalSyncTime());
      buf.append(" ");
    }
    LOG.info(buf);
  }

  /** 
   * Add open lease record to edit log. 
   * Records the block locations of the last block.
   */
  public void logOpenFile(String path, INodeFileUnderConstruction newNode) {

    DeprecatedUTF8 nameReplicationPair[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(path), 
      FSEditLog.toLogReplication(newNode.getReplication()),
      FSEditLog.toLogLong(newNode.getModificationTime()),
      FSEditLog.toLogLong(newNode.getAccessTime()),
      FSEditLog.toLogLong(newNode.getPreferredBlockSize())};
    logEdit(OP_ADD,
            new ArrayWritable(DeprecatedUTF8.class, nameReplicationPair), 
            new ArrayWritable(Block.class, newNode.getBlocks()),
            newNode.getPermissionStatus(),
            new DeprecatedUTF8(newNode.getClientName()),
            new DeprecatedUTF8(newNode.getClientMachine()));
  }

  /** 
   * Add close lease record to edit log.
   */
  public void logCloseFile(String path, INodeFile newNode) {
    DeprecatedUTF8 nameReplicationPair[] = new DeprecatedUTF8[] {
      new DeprecatedUTF8(path),
      FSEditLog.toLogReplication(newNode.getReplication()),
      FSEditLog.toLogLong(newNode.getModificationTime()),
      FSEditLog.toLogLong(newNode.getAccessTime()),
      FSEditLog.toLogLong(newNode.getPreferredBlockSize())};
    logEdit(OP_CLOSE,
            new ArrayWritable(DeprecatedUTF8.class, nameReplicationPair),
            new ArrayWritable(Block.class, newNode.getBlocks()),
            newNode.getPermissionStatus());
  }
  
  /** 
   * Add create directory record to edit log
   */
  public void logMkDir(String path, INode newNode) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] {
      new DeprecatedUTF8(path),
      FSEditLog.toLogLong(newNode.getModificationTime()),
      FSEditLog.toLogLong(newNode.getAccessTime())
    };
    logEdit(OP_MKDIR,
      new ArrayWritable(DeprecatedUTF8.class, info),
      newNode.getPermissionStatus());
  }
  
  /** 
   * Add rename record to edit log
   * TODO: use String parameters until just before writing to disk
   */
  void logRename(String src, String dst, long timestamp) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(src),
      new DeprecatedUTF8(dst),
      FSEditLog.toLogLong(timestamp)};
    logEdit(OP_RENAME_OLD, new ArrayWritable(DeprecatedUTF8.class, info));
  }
  
  /** 
   * Add rename record to edit log
   */
  void logRename(String src, String dst, long timestamp, Options.Rename... options) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(src),
      new DeprecatedUTF8(dst),
      FSEditLog.toLogLong(timestamp)};
    logEdit(OP_RENAME,
      new ArrayWritable(DeprecatedUTF8.class, info),
      toBytesWritable(options));
  }
  
  /** 
   * Add set replication record to edit log
   */
  void logSetReplication(String src, short replication) {
    logEdit(OP_SET_REPLICATION, 
      new DeprecatedUTF8(src), 
      FSEditLog.toLogReplication(replication));
  }
  
  /** Add set namespace quota record to edit log
   * 
   * @param src the string representation of the path to a directory
   * @param quota the directory size limit
   */
  void logSetQuota(String src, long nsQuota, long dsQuota) {
    logEdit(OP_SET_QUOTA,
      new DeprecatedUTF8(src), 
      new LongWritable(nsQuota), new LongWritable(dsQuota));
  }

  /**  Add set permissions record to edit log */
  void logSetPermissions(String src, FsPermission permissions) {
    logEdit(OP_SET_PERMISSIONS, new DeprecatedUTF8(src), permissions);
  }

  /**  Add set owner record to edit log */
  void logSetOwner(String src, String username, String groupname) {
    DeprecatedUTF8 u = new DeprecatedUTF8(username == null? "": username);
    DeprecatedUTF8 g = new DeprecatedUTF8(groupname == null? "": groupname);
    logEdit(OP_SET_OWNER, new DeprecatedUTF8(src), u, g);
  }
  
  /**
   * concat(trg,src..) log
   */
  void logConcat(String trg, String [] srcs, long timestamp) {
    int size = 1 + srcs.length + 1; // trg, srcs, timestamp
    DeprecatedUTF8 info[] = new DeprecatedUTF8[size];
    int idx = 0;
    info[idx++] = new DeprecatedUTF8(trg);
    for(int i=0; i<srcs.length; i++) {
      info[idx++] = new DeprecatedUTF8(srcs[i]);
    }
    info[idx] = FSEditLog.toLogLong(timestamp);
    logEdit(OP_CONCAT_DELETE, new ArrayWritable(DeprecatedUTF8.class, info));
  }
  
  /** 
   * Add delete file record to edit log
   */
  void logDelete(String src, long timestamp) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(src),
      FSEditLog.toLogLong(timestamp)};
    logEdit(OP_DELETE, new ArrayWritable(DeprecatedUTF8.class, info));
  }

  /** 
   * Add generation stamp record to edit log
   */
  void logGenerationStamp(long genstamp) {
    logEdit(OP_SET_GENSTAMP, new LongWritable(genstamp));
  }

  /** 
   * Add access time record to edit log
   */
  void logTimes(String src, long mtime, long atime) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(src),
      FSEditLog.toLogLong(mtime),
      FSEditLog.toLogLong(atime)};
    logEdit(OP_TIMES, new ArrayWritable(DeprecatedUTF8.class, info));
  }

  /** 
   * Add a create symlink record.
   */
  void logSymlink(String path, String value, long mtime, 
                  long atime, INodeSymlink node) {
    DeprecatedUTF8 info[] = new DeprecatedUTF8[] { 
      new DeprecatedUTF8(path),
      new DeprecatedUTF8(value),
      FSEditLog.toLogLong(mtime),
      FSEditLog.toLogLong(atime)};
    logEdit(OP_SYMLINK, 
      new ArrayWritable(DeprecatedUTF8.class, info),
      node.getPermissionStatus());
  }
  
  /**
   * log delegation token to edit log
   * @param id DelegationTokenIdentifier
   * @param expiryTime of the token
   * @return
   */
  void logGetDelegationToken(DelegationTokenIdentifier id,
      long expiryTime) {
    logEdit(OP_GET_DELEGATION_TOKEN, id, FSEditLog.toLogLong(expiryTime));
  }
  
  void logRenewDelegationToken(DelegationTokenIdentifier id,
      long expiryTime) {
    logEdit(OP_RENEW_DELEGATION_TOKEN, id, FSEditLog.toLogLong(expiryTime));
  }
  
  void logCancelDelegationToken(DelegationTokenIdentifier id) {
    logEdit(OP_CANCEL_DELEGATION_TOKEN, id);
  }
  
  void logUpdateMasterKey(DelegationKey key) {
    logEdit(OP_UPDATE_MASTER_KEY, key);
  }
  
  static private DeprecatedUTF8 toLogReplication(short replication) {
    return new DeprecatedUTF8(Short.toString(replication));
  }
  
  static private DeprecatedUTF8 toLogLong(long timestamp) {
    return new DeprecatedUTF8(Long.toString(timestamp));
  }

  /**
   * Return the size of the current EditLog
   */
  //FIXME who uses this, does it make sense with transactions?
  synchronized long getEditLogSize() throws IOException {
    assert getNumEditsDirs() <= getNumEditStreams() : 
        "Number of edits directories should not exceed the number of streams.";
    long size = 0;
    ArrayList<EditLogOutputStream> al = null;
    for (int idx = 0; idx < getNumEditStreams(); idx++) {
      EditLogOutputStream es = editStreams.get(idx);
      try {
        long curSize = es.length();
        assert (size == 0 || size == curSize || curSize ==0) :
          "Wrong streams size";
        size = Math.max(size, curSize);
      } catch (IOException e) {
        LOG.error("getEditLogSize: editstream.length failed. removing editlog (" +
            idx + ") " + es.getName());
        if(al==null) al = new ArrayList<EditLogOutputStream>(1);
        al.add(es);
      }
    }
    if(al!=null) disableAndReportErrorOnStreams(al);
    return size;
  }
  
  /**
   * Return a manifest of what finalized edit logs are available
   */
  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
      throws IOException {
    FSImageTransactionalStorageInspector inspector =
        new FSImageTransactionalStorageInspector();

    for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.EDITS)) {
      inspector.inspectDirectory(sd);
    }
    
    return inspector.getEditLogManifest(sinceTxId);
  }
  
  /**
   * Closes the current edit log and opens edits.new. 
   * @return the transaction id that will be used as the first transaction
   *         in the new log
   */
  synchronized long rollEditLog() throws IOException {
    endCurrentLogSegment();
    
    long nextTxId = getLastWrittenTxId() + 1;
    LOG.info("Rolling edit logs. Next txid after roll will be " + nextTxId);
    lastRollTxId = nextTxId;

    startLogSegment(nextTxId);    
    return nextTxId;
  }

  /**
   * Create empty edit log files.
   * Initialize the output stream for logging.
   * 
   * @throws IOException
   */
  synchronized void startLogSegment(final long txId) {
    LOG.info("Starting log segment at " + txId);
    Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS,
        "Bad state: %s", state);
    lastRollTxId = txId;
    
    numTransactions = totalTimeTransactions = numTransactionsBatchedInSync = 0;

    storage.attemptRestoreRemovedStorage();
    
    mapStreamsAndReportErrors(new StreamClosure() {
      @Override
      public void apply(EditLogOutputStream stream) throws IOException {
	stream.beginLogSegment(txId);
      }
    }, "starting log segment " + txId);

    state = State.IN_SEGMENT;

    logEdit(FSEditLogOpCodes.OP_START_LOG_SEGMENT);
    logSync();    
  }

  synchronized void endCurrentLogSegment() {
    Preconditions.checkState(state == State.IN_SEGMENT,
        "Bad state: %s", state);
    logEdit(FSEditLogOpCodes.OP_END_LOG_SEGMENT);
    waitForSyncToFinish();
    printStatistics(true);
    
    final long lastTxId = getLastWrittenTxId();

    mapStreamsAndReportErrors(new StreamClosure() {
      @Override
      public void apply(EditLogOutputStream stream) throws IOException {
        stream.endLogSegment(lastRollTxId, lastTxId);
      }
    }, "ending log segment");
    
    state = State.BETWEEN_LOG_SEGMENTS;
  }

  /**
   * The actual sync activity happens while not synchronized on this object.
   * Thus, synchronized activities that require that they are not concurrent
   * with file operations should wait for any running sync to finish.
   */
  synchronized void waitForSyncToFinish() {
    while (isSyncRunning) {
      try {
        wait(1000);
      } catch (InterruptedException ie) {}
    }
  }

  /**
   * Return the txid of the last synced transaction.
   * For test use only
   */
  synchronized long getSyncTxId() {
    return synctxid;
  }


  // sets the initial capacity of the flush buffer.
  public void setBufferCapacity(int size) {
    sizeOutputFlushBuffer = size;
  }


  boolean isEmpty() throws IOException {
    return getEditLogSize() <= 0;
  }

  /**
   * Create (or find if already exists) an edit output stream, which
   * streams journal records (edits) to the specified backup node.<br>
   * Send a record, prescribing to start journal spool.<br>
   * This should be sent via regular stream of journal records so that
   * the backup node new exactly after which record it should start spooling.
   * 
   * @param bnReg the backup node registration information.
   * @param nnReg this (active) name-node registration.
   * @throws IOException
   */
  synchronized void logJSpoolStart(NamenodeRegistration bnReg, // backup node
                      NamenodeRegistration nnReg) // active name-node
  throws IOException {
    /*  if(bnReg.isRole(NamenodeRole.CHECKPOINT))
      return; // checkpoint node does not stream edits
    if(editStreams == null)
      editStreams = new ArrayList<EditLogOutputStream>();
    EditLogOutputStream boStream = null;
    for(EditLogOutputStream eStream : editStreams) {
      if(eStream.getName().equals(bnReg.getAddress())) {
        boStream = eStream; // already there
        break;
      }
    }
    if(boStream == null) {
      boStream = new EditLogBackupOutputStream(bnReg, nnReg);
      editStreams.add(boStream);
    }
    logEdit(OP_JSPOOL_START, (Writable[])null);*/
  }

  /**
   * Write an operation to the edit log. Do not sync to persistent
   * store yet.
   */
  synchronized void logEdit(final int length, final byte[] data) {
    if(getNumEditStreams() == 0)
      throw new java.lang.IllegalStateException(NO_JOURNAL_STREAMS_WARNING);
    long start = beginTransaction();

    mapStreamsAndReportErrors(new StreamClosure() {
        @Override
        public void apply(EditLogOutputStream stream) throws IOException {
          stream.write(data, 0, length);
        }
      }, "Writing op to stream");

    endTransaction(start);
  }

  /**
   * Iterates output streams based of the same type.
   * Type null will iterate over all streams.
   */
  private class EditStreamIterator implements Iterator<EditLogOutputStream> {
    JournalType type;
    int prevIndex; // for remove()
    int nextIndex; // for next()

    EditStreamIterator(JournalType streamType) {
      this.type = streamType;
      this.nextIndex = 0;
      this.prevIndex = 0;
    }

    public boolean hasNext() {
      synchronized(FSEditLog.this) {
        if(editStreams == null || 
           editStreams.isEmpty() || nextIndex >= editStreams.size())
          return false;
        while(nextIndex < editStreams.size()
              && !editStreams.get(nextIndex).getType().isOfType(type))
          nextIndex++;
        return nextIndex < editStreams.size();
      }
    }

    public EditLogOutputStream next() {
      EditLogOutputStream stream = null;
      synchronized(FSEditLog.this) {
        stream = editStreams.get(nextIndex);
        prevIndex = nextIndex;
        nextIndex++;
        while(nextIndex < editStreams.size()
            && !editStreams.get(nextIndex).getType().isOfType(type))
        nextIndex++;
      }
      return stream;
    }

    public void remove() {
      nextIndex = prevIndex; // restore previous state
      removeStream(prevIndex); // remove last returned element
      hasNext(); // reset nextIndex to correct place
    }

    void replace(EditLogOutputStream newStream) {
      synchronized (FSEditLog.this) {
        assert 0 <= prevIndex && prevIndex < editStreams.size() :
                                                          "Index out of bound.";
        editStreams.set(prevIndex, newStream);
      }
    }
  }

  /**
   * Get stream iterator for the specified type.
   */
  public Iterator<EditLogOutputStream>
  getOutputStreamIterator(JournalType streamType) {
    return new EditStreamIterator(streamType);
  }

  synchronized void releaseBackupStream(NamenodeRegistration registration) {
    /*  Iterator<EditLogOutputStream> it =
                                  getOutputStreamIterator(JournalType.BACKUP);
    ArrayList<EditLogOutputStream> errorStreams = null;
    NamenodeRegistration backupNode = null;
    while(it.hasNext()) {
      EditLogBackupOutputStream eStream = (EditLogBackupOutputStream)it.next();
      backupNode = eStream.getRegistration();
      if(backupNode.getAddress().equals(registration.getAddress()) &&
            backupNode.isRole(registration.getRole())) {
        errorStreams = new ArrayList<EditLogOutputStream>(1);
        errorStreams.add(eStream);
        break;
      }
    }
    assert backupNode == null || backupNode.isRole(NamenodeRole.BACKUP) :
      "Not a backup node corresponds to a backup stream";
      disableAndReportErrorOnStreams(errorStreams);*/
  }

  synchronized boolean checkBackupRegistration(
      NamenodeRegistration registration) {/*
    Iterator<EditLogOutputStream> it =
                                  getOutputStreamIterator(JournalType.BACKUP);
    boolean regAllowed = !it.hasNext();
    NamenodeRegistration backupNode = null;
    ArrayList<EditLogOutputStream> errorStreams = null;
    while(it.hasNext()) {
      EditLogBackupOutputStream eStream = (EditLogBackupOutputStream)it.next();
      backupNode = eStream.getRegistration();
      if(backupNode.getAddress().equals(registration.getAddress()) &&
          backupNode.isRole(registration.getRole())) {
        regAllowed = true; // same node re-registers
        break;
      }
      if(!eStream.isAlive()) {
        if(errorStreams == null)
          errorStreams = new ArrayList<EditLogOutputStream>(1);
        errorStreams.add(eStream);
        regAllowed = true; // previous backup node failed
      }
    }
    assert backupNode == null || backupNode.isRole(NamenodeRole.BACKUP) :
      "Not a backup node corresponds to a backup stream";
    disableAndReportErrorOnStreams(errorStreams);
    return regAllowed;
					  */
    return false;
  }
  
  static BytesWritable toBytesWritable(Options.Rename... options) {
    byte[] bytes = new byte[options.length];
    for (int i = 0; i < options.length; i++) {
      bytes[i] = options[i].value();
    }
    return new BytesWritable(bytes);
  }

  /**
   * Get the StorageDirectory for a stream
   * @param es Stream whose StorageDirectory we wish to know
   * @return the matching StorageDirectory
   */
  StorageDirectory getStorageDirectoryForStream(EditLogOutputStream es) {
    String parentStorageDir = ((EditLogFileOutputStream)es).getFile().getParentFile().getParentFile().getAbsolutePath();

    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      FSNamesystem.LOG.info("comparing: " + parentStorageDir 
                            + " and " + sd.getRoot().getAbsolutePath()); 
      if (parentStorageDir.equals(sd.getRoot().getAbsolutePath()))
        return sd;
    }
    return null;
  }

  private synchronized void disableStream(EditLogOutputStream stream) {
    try { stream.close(); } catch (IOException e) {
      // nothing to do.
      LOG.warn("Failed to close eStream " + stream.getName()
               + " before removing it (might be ok)");
    }

    editStreams.remove(stream);

    if (editStreams.size() <= 0) {
      String msg = "Fatal Error: All storage directories are inaccessible.";
      LOG.fatal(msg, new IOException(msg));
      Runtime.getRuntime().exit(-1);
    }
  }

  /**
   * Error Handling on a storageDirectory
   *
   */
  // NNStorageListener Interface
  @Override // NNStorageListener
  public synchronized void errorOccurred(StorageDirectory sd)
      throws IOException {
    ArrayList<EditLogOutputStream> errorStreams
      = new ArrayList<EditLogOutputStream>();

    for (EditLogOutputStream eStream : editStreams) {
      LOG.error("Unable to log edits to " + eStream.getName()
                + "; removing it");

      StorageDirectory streamStorageDir = getStorageDirectoryForStream(eStream);
      if (sd == streamStorageDir) {
        errorStreams.add(eStream);
      }
    }

    for (EditLogOutputStream eStream : errorStreams) {
      disableStream(eStream);
    }
  }

  @Override // NNStorageListener
  public synchronized void formatOccurred(StorageDirectory sd)
      throws IOException {
    if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
      URI u = sd.getRoot().toURI();
      try {
        JournalFactory f = new FileJournalFactory(conf, u, storage, sd);
        f.format();
      } catch (Exception e) {
        LOG.error("Exception restoring " + u, e);
      }
    }
  }

  @Override // NNStorageListener
  public synchronized void directoryAvailable(StorageDirectory sd)
      throws IOException {
    if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)
	&& (state == State.IN_SEGMENT)) {
      URI u = sd.getRoot().toURI();
      try {
        JournalFactory f = new FileJournalFactory(conf, u, storage, sd);
        editStreams.add(f.getOutputStream());
      } catch (Exception e) {
        LOG.error("Exception restoring " + u, e);
      }
    }
  }
  
  //// Iteration across streams
  private interface StreamClosure {
    public void apply(EditLogOutputStream jm) throws IOException;
  }
 
  /**
   * Apply the given function across all of the edit streams, disabling
   * any for which the closure throws an IOException.
   * @param status message used for logging errors (e.g. "opening journal")
   */
  private void mapStreamsAndReportErrors(StreamClosure closure, String status) {
    ArrayList<EditLogOutputStream> badStreams = new ArrayList<EditLogOutputStream>();
    for (EditLogOutputStream stream : editStreams) {
      try {
        closure.apply(stream);
      } catch (IOException ioe) {
        LOG.error("Error " + status + " (stream " + stream + ")", ioe);
        badStreams.add(stream);
      }
    }
    disableAndReportErrorOnStreams(badStreams);
  }
}
