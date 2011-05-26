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

import java.util.Map;
import java.util.HashMap;
import java.util.zip.Checksum;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.EOFException;

/**
 * Op codes for edits file, and helper classes
 * for reading the ops from an InputStream.
 * All ops derive from FSEditLogOp and are only 
 * instantiated from #readOp()
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class FSEditLogOp {
  public enum Codes {
    // last op code in file
    OP_INVALID                    ((byte) -1),
    OP_ADD                        ((byte)  0),
    OP_RENAME_OLD                 ((byte)  1), // deprecated operation
    OP_DELETE                     ((byte)  2),
    OP_MKDIR                      ((byte)  3),
    OP_SET_REPLICATION            ((byte)  4),
    @Deprecated OP_DATANODE_ADD   ((byte)  5),
    @Deprecated OP_DATANODE_REMOVE((byte)  6),
    OP_SET_PERMISSIONS            ((byte)  7),
    OP_SET_OWNER                  ((byte)  8),
    OP_CLOSE                      ((byte)  9),
    OP_SET_GENSTAMP               ((byte) 10),
    OP_SET_NS_QUOTA               ((byte) 11), // obsolete
    OP_CLEAR_NS_QUOTA             ((byte) 12), // obsolete
    OP_TIMES                      ((byte) 13), // set atime, mtime
    OP_SET_QUOTA                  ((byte) 14),
    OP_RENAME                     ((byte) 15), // filecontext rename
    OP_CONCAT_DELETE              ((byte) 16), // concat files
    OP_SYMLINK                    ((byte) 17),
    OP_GET_DELEGATION_TOKEN       ((byte) 18),
    OP_RENEW_DELEGATION_TOKEN     ((byte) 19),
    OP_CANCEL_DELEGATION_TOKEN    ((byte) 20),
    OP_UPDATE_MASTER_KEY          ((byte) 21),
    OP_END_LOG_SEGMENT            ((byte) 22),
    OP_START_LOG_SEGMENT          ((byte) 23),

    // must be same as NamenodeProtocol.JA_JSPOOL_START
    OP_JSPOOL_START               ((byte)102);

    private byte opCode;

    /**
     * Constructor
     *
     * @param opCode byte value of constructed enum
     */
    Codes(byte opCode) {
      this.opCode = opCode;
    }

    /**
     * return the byte value of the enum
     *
     * @return the byte value of the enum
     */
    public byte getOpCode() {
      return opCode;
    }

    private static final Map<Byte, Codes> byteToEnum =
      new HashMap<Byte, FSEditLogOp.Codes>();

    static {
      // initialize byte to enum map
      for(Codes opCode : values())
        byteToEnum.put(opCode.getOpCode(), opCode);
    }

    /**
     * Converts byte to FSEditLogOpCodes enum value
     *
     * @param opCode get enum for this opCode
     * @return enum with byte value of opCode
     */
    public static FSEditLogOp.Codes fromByte(byte opCode) {
      return byteToEnum.get(opCode);
    }
  }

  /**
   * Read an operation from an input stream.
   * @param in The stream to read from.
   * @param logVersion The version of the data coming from the stream.
   * @param checksum Checksum being used with input stream.
   * @return the operation read from the stream.
   * @throws IOException on error.
   * @throws EOFException when finished reading file.
   */
  public static FSEditLogOp readOp(DataInputStream in, int logVersion,
                                   Checksum checksum) throws IOException {
    if (checksum != null) {
      checksum.reset();
    }
    in.mark(1);
    byte opCodeByte = in.readByte();
    Codes opCode = FSEditLogOp.Codes.fromByte(opCodeByte);
    if (opCode == FSEditLogOp.Codes.OP_INVALID) {
      in.reset(); // reset back to end of file if somebody reads it again
      throw new EOFException("Reset to end of file");
    }

    long thisTxId = 0;
    if (logVersion <= FSConstants.FIRST_STORED_TXIDS_VERSION) {
      // Read the txid
      thisTxId = in.readLong();
    }

    FSEditLogOp op = null;
    switch (opCode) {
    case OP_ADD:
    case OP_CLOSE:
      op = new AddCloseOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_SET_REPLICATION:
      op = new SetReplicationOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_CONCAT_DELETE:
      op = new ConcatDeleteOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_RENAME_OLD:
      op = new RenameOldOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_DELETE:
      op = new DeleteOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_MKDIR:
      op = new MkdirOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_SET_GENSTAMP:
      op = new SetGenstampOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_DATANODE_ADD:
      op = new DatanodeAddOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_DATANODE_REMOVE:
      op = new DatanodeRemoveOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_SET_PERMISSIONS:
      op = new SetPermissionsOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_SET_OWNER:
      op = new SetOwnerOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_SET_NS_QUOTA:
      op = new SetNSQuotaOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_CLEAR_NS_QUOTA:
      op = new ClearNSQuotaOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_SET_QUOTA:
      op = new SetQuotaOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_TIMES:
      op = new TimesOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_SYMLINK:
      op = new SymlinkOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_RENAME:
      op = new RenameOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_GET_DELEGATION_TOKEN:
      op = new GetDelegationTokenOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_RENEW_DELEGATION_TOKEN:
      op = new RenewDelegationTokenOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_CANCEL_DELEGATION_TOKEN:
      op = new CancelDelegationTokenOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_UPDATE_MASTER_KEY:
      op = new UpdateMasterKeyOp(opCode, thisTxId, in, logVersion);
      break;
    case OP_START_LOG_SEGMENT:
    case OP_END_LOG_SEGMENT:
      op = new LogSegmentOp(opCode, thisTxId, in, logVersion);
      break;
    default:
      throw new IOException("Never seen opCode " + opCode);
    }
    validateChecksum(in, checksum, thisTxId);
    return op;
  }

  /**
   * Validate a transaction's checksum
   */
  private static void validateChecksum(DataInputStream in,
                                       Checksum checksum, long tid)
      throws IOException {
    if (checksum != null) {
      int calculatedChecksum = (int)checksum.getValue();
      int readChecksum = in.readInt(); // read in checksum
      if (readChecksum != calculatedChecksum) {
        throw new ChecksumException(
            "Transaction " + tid + " is corrupt. Calculated checksum is " +
            calculatedChecksum + " but read checksum " + readChecksum, tid);
      }
    }
  }

  final Codes opCode;
  final long txid;

  /**
   * Constructor for an EditLog Op. EditLog ops cannot be constructed
   * directly, but only through #readOp.
   */
  private FSEditLogOp(Codes opCode, long txid) {
    this.opCode = opCode;
    this.txid = txid;
  }

  static class AddCloseOp extends FSEditLogOp {
    final int length;
    final String path;
    final short replication;
    final long mtime;
    final long atime;
    final long blockSize;
    final BlockInfo[] blocks;
    final PermissionStatus permissions;
    final String clientName;
    final String clientMachine;
    //final DatanodeDescriptor[] dataNodeDescriptors; UNUSED

    private AddCloseOp(Codes opCode, long txid,
                       DataInputStream in, int logVersion) throws IOException {
      super(opCode, txid);
      // versions > 0 support per file replication
      // get name and replication
      this.length = in.readInt();
      if (-7 == logVersion && length != 3||
          -17 < logVersion && logVersion < -7 && length != 4 ||
          logVersion <= -17 && length != 5) {
        throw new IOException("Incorrect data format."  +
                              " logVersion is " + logVersion +
                              " but writables.length is " +
                              length + ". ");
      }
      this.path = FSImageSerialization.readString(in);
      this.replication = readShort(in);
      this.mtime = readLong(in);
      if (logVersion <= -17) {
        this.atime = readLong(in);
      } else {
        this.atime = 0;
      }
      if (logVersion < -7) {
        this.blockSize = readLong(in);
      } else {
        this.blockSize = 0;
      }

      // get blocks
      boolean isFileUnderConstruction
        = (this.opCode == FSEditLogOp.Codes.OP_ADD);
      this.blocks =
        readBlocks(in, logVersion, isFileUnderConstruction, this.replication);

      if (logVersion <= -11) {
        this.permissions = PermissionStatus.read(in);
      } else {
        this.permissions = null;
      }

      // clientname, clientMachine and block locations of last block.
      if (this.opCode == FSEditLogOp.Codes.OP_ADD && logVersion <= -12) {
        this.clientName = FSImageSerialization.readString(in);
        this.clientMachine = FSImageSerialization.readString(in);
        if (-13 <= logVersion) {
          readDatanodeDescriptorArray(in);
        }
      } else {
        this.clientName = "";
        this.clientMachine = "";
      }
    }

    /** This method is defined for compatibility reason. */
    private DatanodeDescriptor[] readDatanodeDescriptorArray(DataInput in)
        throws IOException {
      DatanodeDescriptor[] locations = new DatanodeDescriptor[in.readInt()];
        for (int i = 0; i < locations.length; i++) {
          locations[i] = new DatanodeDescriptor();
          locations[i].readFieldsFromFSEditLog(in);
        }
        return locations;
    }

    private BlockInfo[] readBlocks(
        DataInputStream in,
        int logVersion,
        boolean isFileUnderConstruction,
        short replication) throws IOException {
      int numBlocks = in.readInt();
      BlockInfo[] blocks = new BlockInfo[numBlocks];
      Block blk = new Block();
      BlockTwo oldblk = new BlockTwo();
      for (int i = 0; i < numBlocks; i++) {
        if (logVersion <= -14) {
          blk.readFields(in);
        } else {
          oldblk.readFields(in);
          blk.set(oldblk.blkid, oldblk.len,
                  GenerationStamp.GRANDFATHER_GENERATION_STAMP);
        }
        if(isFileUnderConstruction && i == numBlocks-1)
          blocks[i] = new BlockInfoUnderConstruction(blk, replication);
        else
          blocks[i] = new BlockInfo(blk, replication);
      }
      return blocks;
    }
  }

  static class SetReplicationOp extends FSEditLogOp {
    final String path;
    final short replication;

    private SetReplicationOp(Codes opCode, long txid,
                             DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);
      this.path = FSImageSerialization.readString(in);
      this.replication = readShort(in);
    }
  }

  static class ConcatDeleteOp extends FSEditLogOp {
    final int length;
    final String trg;
    final String[] srcs;
    final long timestamp;

    private ConcatDeleteOp(Codes opCode, long txid,
                           DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);
      if (logVersion > -22) {
        throw new IOException("Unexpected opCode " + opCode
                              + " for version " + logVersion);
      }

      this.length = in.readInt();
      if (length < 3) { // trg, srcs.., timestam
        throw new IOException("Incorrect data format. "
                              + "Mkdir operation.");
      }
      this.trg = FSImageSerialization.readString(in);
      int srcSize = this.length - 1 - 1; //trg and timestamp
      this.srcs = new String [srcSize];
      for(int i=0; i<srcSize;i++) {
        srcs[i]= FSImageSerialization.readString(in);
      }
      this.timestamp = readLong(in);
    }
  }

  static class RenameOldOp extends FSEditLogOp {
    final int length;
    final String src;
    final String dst;
    final long timestamp;

    private RenameOldOp(Codes opCode, long txid,
                        DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);
      this.length = in.readInt();
      if (this.length != 3) {
        throw new IOException("Incorrect data format. "
                              + "Mkdir operation.");
      }
      this.src = FSImageSerialization.readString(in);
      this.dst = FSImageSerialization.readString(in);
      this.timestamp = readLong(in);
    }
  }

  static class DeleteOp extends FSEditLogOp {
    final int length;
    final String path;
    final long timestamp;

    private DeleteOp(Codes opCode, long txid,
                     DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);

      this.length = in.readInt();
      if (this.length != 2) {
        throw new IOException("Incorrect data format. "
                              + "delete operation.");
      }
      this.path = FSImageSerialization.readString(in);
      this.timestamp = readLong(in);
    }
  }

  static class MkdirOp extends FSEditLogOp {
    final int length;
    final String path;
    final long timestamp;
    final long atime;
    final PermissionStatus permissions;

    private MkdirOp(Codes opCode, long txid,
                    DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);

      this.length = in.readInt();
      if (-17 < logVersion && length != 2 ||
          logVersion <= -17 && length != 3) {
        throw new IOException("Incorrect data format. "
                              + "Mkdir operation.");
      }
      this.path = FSImageSerialization.readString(in);
      this.timestamp = readLong(in);

      // The disk format stores atimes for directories as well.
      // However, currently this is not being updated/used because of
      // performance reasons.
      if (logVersion <= -17) {
        this.atime = readLong(in);
      } else {
        this.atime = 0;
      }

      if (logVersion <= -11) {
        this.permissions = PermissionStatus.read(in);
      } else {
        this.permissions = null;
      }
    }
  }

  static class SetGenstampOp extends FSEditLogOp {
    final long lw;

    private SetGenstampOp(Codes opCode, long txid,
                          DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);
      this.lw = in.readLong();
    }
  }

  static class DatanodeAddOp extends FSEditLogOp {
    private DatanodeAddOp(Codes opCode, long txid,
                          DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);
      //Datanodes are not persistent any more.
      FSImageSerialization.DatanodeImage.skipOne(in);
    }
  }

  static class DatanodeRemoveOp extends FSEditLogOp {
    private DatanodeRemoveOp(Codes opCode, long txid,
                             DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);
      DatanodeID nodeID = new DatanodeID();
      nodeID.readFields(in);
      //Datanodes are not persistent any more.
    }
  }

  static class SetPermissionsOp extends FSEditLogOp {
    final String src;
    final FsPermission permissions;

    private SetPermissionsOp(Codes opCode, long txid,
                             DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);
      if (logVersion > -11)
        throw new IOException("Unexpected opCode " + opCode
                              + " for version " + logVersion);
      this.src = FSImageSerialization.readString(in);
      this.permissions = FsPermission.read(in);
    }
  }

  static class SetOwnerOp extends FSEditLogOp {
    final String src;
    final String username;
    final String groupname;

    private SetOwnerOp(Codes opCode, long txid,
                       DataInputStream in, int logVersion) throws IOException {
      super(opCode, txid);

      if (logVersion > -11)
        throw new IOException("Unexpected opCode " + opCode
                              + " for version " + logVersion);
      this.src = FSImageSerialization.readString(in);
      this.username = FSImageSerialization.readString_EmptyAsNull(in);
      this.groupname = FSImageSerialization.readString_EmptyAsNull(in);
    }

  }

  static class SetNSQuotaOp extends FSEditLogOp {
    final String src;
    final long nsQuota;

    private SetNSQuotaOp(Codes opCode, long txid,
                         DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);

      if (logVersion > -16) {
        throw new IOException("Unexpected opCode " + opCode
                              + " for version " + logVersion);
      }

      this.src = FSImageSerialization.readString(in);
      this.nsQuota = readLongWritable(in);
    }
  }

  static class ClearNSQuotaOp extends FSEditLogOp {
    final String src;

    private ClearNSQuotaOp(Codes opCode, long txid,
                           DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);
      if (logVersion > -16) {
        throw new IOException("Unexpected opCode " + opCode
                              + " for version " + logVersion);
      }
      this.src = FSImageSerialization.readString(in);
    }
  }

  static class SetQuotaOp extends FSEditLogOp {
    final String src;
    final long nsQuota;
    final long dsQuota;

    private SetQuotaOp(Codes opCode, long txid,
                       DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);

      this.src = FSImageSerialization.readString(in);
      this.nsQuota = readLongWritable(in);
      this.dsQuota = readLongWritable(in);
    }
  }

  static class TimesOp extends FSEditLogOp {
    final int length;
    final String path;
    final long mtime;
    final long atime;

    private TimesOp(Codes opCode, long txid,
                    DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);

      this.length = in.readInt();
      if (length != 3) {
        throw new IOException("Incorrect data format. "
                              + "times operation.");
      }
      this.path = FSImageSerialization.readString(in);
      this.mtime = readLong(in);
      this.atime = readLong(in);
    }
  }

  static class SymlinkOp extends FSEditLogOp {
    final int length;
    final String path;
    final String value;
    final long mtime;
    final long atime;
    final PermissionStatus permissionStatus;

    private SymlinkOp(Codes opCode, long txid,
                      DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);

      this.length = in.readInt();
      if (this.length != 4) {
        throw new IOException("Incorrect data format. "
                              + "symlink operation.");
      }
      this.path = FSImageSerialization.readString(in);
      this.value = FSImageSerialization.readString(in);
      this.mtime = readLong(in);
      this.atime = readLong(in);
      this.permissionStatus = PermissionStatus.read(in);
    }
  }

  static class RenameOp extends FSEditLogOp {
    final int length;
    final String src;
    final String dst;
    final long timestamp;
    final Rename[] options;

    private RenameOp(Codes opCode, long txid,
                     DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);
      if (logVersion > -21) {
        throw new IOException("Unexpected opCode " + opCode
                              + " for version " + logVersion);
      }

      this.length = in.readInt();
      if (this.length != 3) {
        throw new IOException("Incorrect data format. "
                              + "Mkdir operation.");
      }
      this.src = FSImageSerialization.readString(in);
      this.dst = FSImageSerialization.readString(in);
      this.timestamp = readLong(in);
      this.options = readRenameOptions(in);
    }

    private Rename[] readRenameOptions(DataInputStream in) throws IOException {
      BytesWritable writable = new BytesWritable();
      writable.readFields(in);

      byte[] bytes = writable.getBytes();
      Rename[] options = new Rename[bytes.length];

      for (int i = 0; i < bytes.length; i++) {
        options[i] = Rename.valueOf(bytes[i]);
      }
      return options;
    }
  }

  static class GetDelegationTokenOp extends FSEditLogOp {
    final DelegationTokenIdentifier token;
    final long expiryTime;

    private GetDelegationTokenOp(Codes opCode, long txid,
                                 DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);

      if (logVersion > -24) {
        throw new IOException("Unexpected opCode " + opCode
                              + " for version " + logVersion);
      }
      this.token = new DelegationTokenIdentifier();
      this.token.readFields(in);
      this.expiryTime = readLong(in);
    }
  }

  static class RenewDelegationTokenOp extends FSEditLogOp {
    final DelegationTokenIdentifier token;
    final long expiryTime;

    private RenewDelegationTokenOp(Codes opCode, long txid,
                                   DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);

      if (logVersion > -24) {
        throw new IOException("Unexpected opCode " + opCode
                              + " for version " + logVersion);
      }
      this.token = new DelegationTokenIdentifier();
      this.token.readFields(in);
      this.expiryTime = readLong(in);
    }
  }

  static class CancelDelegationTokenOp extends FSEditLogOp {
    final DelegationTokenIdentifier token;

    private CancelDelegationTokenOp(Codes opCode, long txid,
                                    DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);

      if (logVersion > -24) {
        throw new IOException("Unexpected opCode " + opCode
                              + " for version " + logVersion);
      }
      this.token = new DelegationTokenIdentifier();
      this.token.readFields(in);
    }
  }

  static class UpdateMasterKeyOp extends FSEditLogOp {
    final DelegationKey key;

    private UpdateMasterKeyOp(Codes opCode, long txid,
                              DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);
      if (logVersion > -24) {
        throw new IOException("Unexpected opCode " + opCode
                              + " for version " + logVersion);
      }

      this.key = new DelegationKey();
      this.key.readFields(in);
    }
  }

  static class LogSegmentOp extends FSEditLogOp {
    private LogSegmentOp(Codes opCode, long txid,
                         DataInputStream in, int logVersion)
        throws IOException {
      super(opCode, txid);
      if (logVersion > FSConstants.FIRST_TXNID_BASED_LAYOUT_VERSION) {
        throw new IOException("Unexpected opCode " + opCode
                              + " for version " + logVersion);
      }
    }
  }

  static private short readShort(DataInputStream in) throws IOException {
    return Short.parseShort(FSImageSerialization.readString(in));
  }

  static private long readLong(DataInputStream in) throws IOException {
    return Long.parseLong(FSImageSerialization.readString(in));
  }

  /**
   * A class to read in blocks stored in the old format. The only two
   * fields in the block were blockid and length.
   */
  static class BlockTwo implements Writable {
    long blkid;
    long len;

    static {                                      // register a ctor
      WritableFactories.setFactory
        (BlockTwo.class,
         new WritableFactory() {
           public Writable newInstance() { return new BlockTwo(); }
         });
    }


    BlockTwo() {
      blkid = 0;
      len = 0;
    }
    /////////////////////////////////////
    // Writable
    /////////////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeLong(blkid);
      out.writeLong(len);
    }

    public void readFields(DataInput in) throws IOException {
      this.blkid = in.readLong();
      this.len = in.readLong();
    }
  }

    // a place holder for reading a long
  private static final LongWritable longWritable = new LongWritable();

  /** Read an integer from an input stream */
  private static long readLongWritable(DataInputStream in) throws IOException {
    synchronized (longWritable) {
      longWritable.readFields(in);
      return longWritable.get();
    }
  }
}
