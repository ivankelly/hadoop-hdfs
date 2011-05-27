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

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import static org.apache.hadoop.hdfs.server.common.Util.now;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.*;

public class FSEditLogLoader {
  private final FSNamesystem fsNamesys;

  public FSEditLogLoader() {
    this.fsNamesys = null;
  }

  public FSEditLogLoader(FSNamesystem fsNamesys) {
    this.fsNamesys = fsNamesys;
  }
  
  /**
   * Load an edit log, and apply the changes to the in-memory structure
   * This is where we apply edits that we've been writing to disk all
   * along.
   */
  int loadFSEdits(EditLogInputStream edits, long expectedStartingTxId)
  throws IOException {
    long startTime = now();
    int numEdits = loadFSEdits(edits, true, expectedStartingTxId);
    FSImage.LOG.info("Edits file " + edits.getName() 
        + " of size " + edits.length() + " edits # " + numEdits 
        + " loaded in " + (now()-startTime)/1000 + " seconds.");
    return numEdits;
  }

  /**
   * Read the header of fsedit log
   * @param in fsedit stream
   * @return the edit log version number
   * @throws IOException if error occurs
   */
  int readLogVersion(DataInputStream in) throws IOException {
    int logVersion = 0;
    // Read log file version. Could be missing.
    in.mark(4);
    // If edits log is greater than 2G, available method will return negative
    // numbers, so we avoid having to call available
    boolean available = true;
    try {
      logVersion = in.readByte();
    } catch (EOFException e) {
      available = false;
    }
    if (available) {
      in.reset();
      logVersion = in.readInt();
      if (logVersion < FSConstants.LAYOUT_VERSION) // future version
        throw new IOException(
            "Unexpected version of the file system log file: "
            + logVersion + ". Current version = "
            + FSConstants.LAYOUT_VERSION + ".");
    }
    assert logVersion <= Storage.LAST_UPGRADABLE_LAYOUT_VERSION :
      "Unsupported version " + logVersion;
    return logVersion;
  }

  int loadFSEdits(EditLogInputStream edits, boolean closeOnExit,
      long expectedStartingTxId)
  throws IOException {
    BufferedInputStream bin = new BufferedInputStream(edits);
    DataInputStream in = new DataInputStream(bin);

    int numEdits = 0;
    int logVersion = 0;

    try {
      logVersion = readLogVersion(in);
      Checksum checksum = null;
      if (logVersion <= -28) { // support fsedits checksum
        checksum = FSEditLog.getChecksum();
        in = new DataInputStream(new CheckedInputStream(bin, checksum));
      }

      numEdits = loadEditRecords(logVersion, in, checksum, false,
          expectedStartingTxId);
    } finally {
      if(closeOnExit)
        in.close();
    }

    return numEdits;
  }

  @SuppressWarnings("deprecation,unchecked")
  int loadEditRecords(int logVersion, DataInputStream in,
                      Checksum checksum, boolean closeOnExit,
                      long expectedStartingTxId)
      throws IOException {
    FSDirectory fsDir = fsNamesys.dir;
    int numEdits = 0;

    int numOpAdd = 0, numOpClose = 0, numOpDelete = 0,
        numOpRenameOld = 0, numOpSetRepl = 0, numOpMkDir = 0,
        numOpSetPerm = 0, numOpSetOwner = 0, numOpSetGenStamp = 0,
        numOpTimes = 0, numOpRename = 0, numOpConcatDelete = 0, 
        numOpSymlink = 0, numOpGetDelegationToken = 0,
        numOpRenewDelegationToken = 0, numOpCancelDelegationToken = 0, 
        numOpUpdateMasterKey = 0, numOpOther = 0;

    // Keep track of the file offsets of the last several opcodes.
    // This is handy when manually recovering corrupted edits files.
    PositionTrackingInputStream tracker = new PositionTrackingInputStream(in);
    in = new DataInputStream(tracker);
    long recentOpcodeOffsets[] = new long[4];
    Arrays.fill(recentOpcodeOffsets, -1);

    try {
      long txId = expectedStartingTxId - 1;

      try {
        while (true) {
          FSEditLogOp op = null;
          try {
            op = FSEditLogOp.readOp(in, logVersion, checksum);
          } catch (EOFException e) {
            break; // no more transactions
          }

          recentOpcodeOffsets[numEdits % recentOpcodeOffsets.length] =
              tracker.getPos();
          if (logVersion <= FSConstants.FIRST_STORED_TXIDS_VERSION) {
            // Read the txid
            long thisTxId = op.txid;
            if (thisTxId != txId + 1) {
              throw new IOException("Expected transaction ID " +
                  (txId + 1) + " but got " + thisTxId);
            }
            txId = thisTxId;
          }

          numEdits++;
          switch (op.opCode) {
          case OP_ADD:
          case OP_CLOSE: {
            AddCloseOp addcloseop = (AddCloseOp)op;

            // versions > 0 support per file replication
            // get name and replication
            int length = addcloseop.length;
            short replication
              = fsNamesys.adjustReplication(addcloseop.replication);

            long blockSize = addcloseop.blockSize;
            BlockInfo blocks[] = addcloseop.blocks;

            PermissionStatus permissions = fsNamesys.getUpgradePermission();
            if (addcloseop.permissions != null) {
              permissions = addcloseop.permissions;
            }


            // Older versions of HDFS does not store the block size in inode.
            // If the file has more than one block, use the size of the
            // first block as the blocksize. Otherwise use the default
            // block size.
            if (-8 <= logVersion && blockSize == 0) {
              if (blocks.length > 1) {
                blockSize = blocks[0].getNumBytes();
              } else {
                long first = ((blocks.length == 1)? blocks[0].getNumBytes(): 0);
                blockSize = Math.max(fsNamesys.getDefaultBlockSize(), first);
              }
            }


            // The open lease transaction re-creates a file if necessary.
            // Delete the file if it already exists.
            if (FSNamesystem.LOG.isDebugEnabled()) {
              FSNamesystem.LOG.debug(op.opCode + ": " + addcloseop.path +
                  " numblocks : " + blocks.length +
                  " clientHolder " + addcloseop.clientName +
                  " clientMachine " + addcloseop.clientMachine);
            }

            fsDir.unprotectedDelete(addcloseop.path, addcloseop.mtime);

            // add to the file tree
            INodeFile node = (INodeFile)fsDir.unprotectedAddFile(
                addcloseop.path, permissions,
                blocks, replication,
                addcloseop.mtime, addcloseop.atime, blockSize);
            if (addcloseop.opCode == FSEditLogOp.Codes.OP_ADD) {
              numOpAdd++;
              //
              // Replace current node with a INodeUnderConstruction.
              // Recreate in-memory lease record.
              //
              INodeFileUnderConstruction cons = new INodeFileUnderConstruction(
                                        node.getLocalNameBytes(),
                                        node.getReplication(),
                                        node.getModificationTime(),
                                        node.getPreferredBlockSize(),
                                        node.getBlocks(),
                                        node.getPermissionStatus(),
                                        addcloseop.clientName,
                                        addcloseop.clientMachine,
                                        null);
              fsDir.replaceNode(addcloseop.path, node, cons);
              fsNamesys.leaseManager.addLease(cons.getClientName(),
                                              addcloseop.path);
            }
            break;
          }
          case OP_SET_REPLICATION: {
            numOpSetRepl++;
            SetReplicationOp setrepop = (SetReplicationOp)op;
            short replication
              = fsNamesys.adjustReplication(setrepop.replication);
            fsDir.unprotectedSetReplication(setrepop.path, replication, null);
            break;
          }
          case OP_CONCAT_DELETE: {
            numOpConcatDelete++;

            ConcatDeleteOp concatdelop = (ConcatDeleteOp)op;
            fsDir.unprotectedConcat(concatdelop.trg, concatdelop.srcs);
            break;
          }
          case OP_RENAME_OLD: {
            numOpRenameOld++;
            RenameOldOp renameop = (RenameOldOp)op;
            HdfsFileStatus dinfo = fsDir.getFileInfo(renameop.dst, false);
            fsDir.unprotectedRenameTo(renameop.src, renameop.dst,
                                      renameop.timestamp);
            fsNamesys.changeLease(renameop.src, renameop.dst, dinfo);
            break;
          }
          case OP_DELETE: {
            numOpDelete++;

            DeleteOp deleteop = (DeleteOp)op;
            fsDir.unprotectedDelete(deleteop.path, deleteop.timestamp);
            break;
          }
          case OP_MKDIR: {
            numOpMkDir++;
            MkdirOp mkdirop = (MkdirOp)op;
            PermissionStatus permissions = fsNamesys.getUpgradePermission();
            if (mkdirop.permissions != null) {
              permissions = mkdirop.permissions;
            }

            fsDir.unprotectedMkdir(mkdirop.path, permissions,
                                   mkdirop.timestamp);
            break;
          }
          case OP_SET_GENSTAMP: {
            numOpSetGenStamp++;
            SetGenstampOp sgop = (SetGenstampOp)op;
            fsNamesys.setGenerationStamp(sgop.lw);
            break;
          }
          case OP_SET_PERMISSIONS: {
            numOpSetPerm++;

            SetPermissionsOp spop = (SetPermissionsOp)op;
            fsDir.unprotectedSetPermission(spop.src, spop.permissions);
            break;
          }
          case OP_SET_OWNER: {
            numOpSetOwner++;

            SetOwnerOp soop = (SetOwnerOp)op;
            fsDir.unprotectedSetOwner(soop.src, soop.username, soop.groupname);
            break;
          }
          case OP_SET_NS_QUOTA: {
            SetNSQuotaOp snqop = (SetNSQuotaOp)op;
            fsDir.unprotectedSetQuota(snqop.src,
                                      snqop.nsQuota,
                                      FSConstants.QUOTA_DONT_SET);
            break;
          }
          case OP_CLEAR_NS_QUOTA: {
            ClearNSQuotaOp cnqop = (ClearNSQuotaOp)op;
            fsDir.unprotectedSetQuota(cnqop.src,
                                      FSConstants.QUOTA_RESET,
                                      FSConstants.QUOTA_DONT_SET);
            break;
          }

          case OP_SET_QUOTA:
            SetQuotaOp sqop = (SetQuotaOp)op;
            fsDir.unprotectedSetQuota(sqop.src,
                                      sqop.nsQuota,
                                      sqop.dsQuota);
            break;

          case OP_TIMES: {
            numOpTimes++;
            TimesOp top = (TimesOp)op;

            fsDir.unprotectedSetTimes(top.path, top.mtime, top.atime, true);
            break;
          }
          case OP_SYMLINK: {
            numOpSymlink++;

            SymlinkOp symop = (SymlinkOp)op;
            fsDir.unprotectedSymlink(symop.path, symop.value, symop.mtime,
                                     symop.atime, symop.permissionStatus);
            break;
          }
          case OP_RENAME: {
            numOpRename++;
            RenameOp rop = (RenameOp)op;

            HdfsFileStatus dinfo = fsDir.getFileInfo(rop.dst, false);
            fsDir.unprotectedRenameTo(rop.src, rop.dst,
                                      rop.timestamp, rop.options);
            fsNamesys.changeLease(rop.src, rop.dst, dinfo);
            break;
          }
          case OP_GET_DELEGATION_TOKEN: {
            numOpGetDelegationToken++;
            GetDelegationTokenOp gdtop = (GetDelegationTokenOp)op;

            fsNamesys.getDelegationTokenSecretManager()
              .addPersistedDelegationToken(gdtop.token, gdtop.expiryTime);
            break;
          }
          case OP_RENEW_DELEGATION_TOKEN: {
            numOpRenewDelegationToken++;

            RenewDelegationTokenOp rdtop = (RenewDelegationTokenOp)op;
            fsNamesys.getDelegationTokenSecretManager()
              .updatePersistedTokenRenewal(rdtop.token, rdtop.expiryTime);
            break;
          }
          case OP_CANCEL_DELEGATION_TOKEN: {
            numOpCancelDelegationToken++;

            CancelDelegationTokenOp cdtop = (CancelDelegationTokenOp)op;
            fsNamesys.getDelegationTokenSecretManager()
                .updatePersistedTokenCancellation(cdtop.token);
            break;
          }
          case OP_UPDATE_MASTER_KEY: {
            numOpUpdateMasterKey++;
            UpdateMasterKeyOp ymkop = (UpdateMasterKeyOp)op;
            fsNamesys.getDelegationTokenSecretManager()
              .updatePersistedMasterKey(ymkop.key);
            break;
          }
          case OP_DATANODE_ADD:
          case OP_DATANODE_REMOVE:
          case OP_START_LOG_SEGMENT:
          case OP_END_LOG_SEGMENT:
            numOpOther++;
            break;
          default:
            throw new IOException("Invalid operation read " + op.opCode);
          }
        }
      } catch (IOException ex) {
        check203UpgradeFailure(logVersion, ex);
      } finally {
        if(closeOnExit)
          in.close();
      }
    } catch (Throwable t) {
      // Catch Throwable because in the case of a truly corrupt edits log, any
      // sort of error might be thrown (NumberFormat, NullPointer, EOF, etc.)
      StringBuilder sb = new StringBuilder();
      sb.append("Error replaying edit log at offset " + tracker.getPos());
      if (recentOpcodeOffsets[0] != -1) {
        Arrays.sort(recentOpcodeOffsets);
        sb.append("\nRecent opcode offsets:");
        for (long offset : recentOpcodeOffsets) {
          if (offset != -1) {
            sb.append(' ').append(offset);
          }
        }
      }
      String errorMessage = sb.toString();
      FSImage.LOG.error(errorMessage);
      throw new IOException(errorMessage, t);
    }
    if (FSImage.LOG.isDebugEnabled()) {
      FSImage.LOG.debug("numOpAdd = " + numOpAdd + " numOpClose = " + numOpClose 
          + " numOpDelete = " + numOpDelete 
          + " numOpRenameOld = " + numOpRenameOld 
          + " numOpSetRepl = " + numOpSetRepl + " numOpMkDir = " + numOpMkDir
          + " numOpSetPerm = " + numOpSetPerm 
          + " numOpSetOwner = " + numOpSetOwner
          + " numOpSetGenStamp = " + numOpSetGenStamp 
          + " numOpTimes = " + numOpTimes
          + " numOpConcatDelete  = " + numOpConcatDelete
          + " numOpRename = " + numOpRename
          + " numOpGetDelegationToken = " + numOpGetDelegationToken
          + " numOpRenewDelegationToken = " + numOpRenewDelegationToken
          + " numOpCancelDelegationToken = " + numOpCancelDelegationToken
          + " numOpUpdateMasterKey = " + numOpUpdateMasterKey
          + " numOpOther = " + numOpOther);
    }
    return numEdits;
  }

  /**
   * Throw appropriate exception during upgrade from 203, when editlog loading
   * could fail due to opcode conflicts.
   */
  private void check203UpgradeFailure(int logVersion, IOException ex)
      throws IOException {
    // 0.20.203 version version has conflicting opcodes with the later releases.
    // The editlog must be emptied by restarting the namenode, before proceeding
    // with the upgrade.
    if (Storage.is203LayoutVersion(logVersion)
        && logVersion != FSConstants.LAYOUT_VERSION) {
      String msg = "During upgrade failed to load the editlog version "
          + logVersion + " from release 0.20.203. Please go back to the old "
          + " release and restart the namenode. This empties the editlog "
          + " and saves the namespace. Resume the upgrade after this step.";
      throw new IOException(msg, ex);
    } else {
      throw ex;
    }
  }
  
  /**
   * Stream wrapper that keeps track of the current file position.
   */
  private static class PositionTrackingInputStream extends FilterInputStream {
    private long curPos = 0;
    private long markPos = -1;

    public PositionTrackingInputStream(InputStream is) {
      super(is);
    }

    public int read() throws IOException {
      int ret = super.read();
      if (ret != -1) curPos++;
      return ret;
    }

    public int read(byte[] data) throws IOException {
      int ret = super.read(data);
      if (ret > 0) curPos += ret;
      return ret;
    }

    public int read(byte[] data, int offset, int length) throws IOException {
      int ret = super.read(data, offset, length);
      if (ret > 0) curPos += ret;
      return ret;
    }

    public void mark(int limit) {
      super.mark(limit);
      markPos = curPos;
    }

    public void reset() throws IOException {
      if (markPos == -1) {
        throw new IOException("Not marked!");
      }
      super.reset();
      curPos = markPos;
      markPos = -1;
    }

    public long getPos() {
      return curPos;
    }
  }
}
