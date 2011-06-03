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

import java.io.File;
import java.io.IOException;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Journal manager for the common case of edits files being written
 * to a storage directory.
 *
 * Note: this class is not thread-safe and should be externally
 * synchronized.
 */
public class FileJournalManager implements JournalManager {
  private static final Log LOG = LogFactory.getLog(FileJournalManager.class);

  private final StorageDirectory sd;
  private int outputBufferCapacity = 512*1024;
  private static final Pattern EDITS_REGEX = Pattern.compile(
      NameNodeFile.EDITS.getName() + "_(\\d+)-(\\d+)");
  private static final Pattern EDITS_INPROGRESS_REGEX = Pattern.compile(
      NameNodeFile.EDITS_INPROGRESS.getName() + "_(\\d+)");

  public FileJournalManager(StorageDirectory sd) {
    this.sd = sd;
  }

  @Override
  public EditLogOutputStream startLogSegment(long txid) throws IOException {
    File newInProgress = NNStorage.getInProgressEditsFile(sd, txid);
    EditLogOutputStream stm = new EditLogFileOutputStream(newInProgress,
        outputBufferCapacity);
    stm.create();
    return stm;
  }

  @Override
  public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    File inprogressFile = NNStorage.getInProgressEditsFile(
        sd, firstTxId);
    File dstFile = NNStorage.getFinalizedEditsFile(
        sd, firstTxId, lastTxId);
    LOG.debug("Finalizing edits file " + inprogressFile + " -> " + dstFile);

    Preconditions.checkState(!dstFile.exists(),
        "Can't finalize edits file " + inprogressFile
        + " since finalized file "
        + "already exists");
    if (!inprogressFile.renameTo(dstFile)) {
      throw new IOException("Unable to finalize edits file " + inprogressFile);
    }
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
    this.outputBufferCapacity = size;
  }

  @Override
  public EditLogInputStream getInputStream(long fromTxId) throws IOException {
    for (EditLogFile elf : getLogFiles(fromTxId)) {
      if (elf.startTxId == fromTxId) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Returning edit stream reading from " + elf.file);
        }
        return new EditLogFileInputStream(elf.file);
      }
    }

    throw new IOException("Cannot find editlog file with " + fromTxId
                          + " as first first txid");
  }

  @Override
  public long getNumberOfTransactions(long fromTxId) throws IOException {
    long numTxns = 0L;

    for (EditLogFile elf : getLogFiles(fromTxId)) {
      if (elf.startTxId > fromTxId) { // there must be a gap
        throw new IOException("Gap in transactions "
                              + fromTxId + " - " + elf.startTxId);
      } else if (fromTxId == elf.startTxId) {
        fromTxId = elf.endTxId + 1;
        numTxns += fromTxId - elf.startTxId;
      } // else skip
    }

    return numTxns;
  }

  @Override
  public void recoverUnclosedStreams() throws IOException {
    File currentDir = sd.getCurrentDir();
    for (File f : currentDir.listFiles()) {
      // Check for in-progress edits
      Matcher inProgressEditsMatch
        = EDITS_INPROGRESS_REGEX.matcher(f.getName());
      if (inProgressEditsMatch.matches()) {
        boolean corrupt = false;
        long startTxId = -1, endTxId = -1;
        int logVersion = 0;

        BufferedInputStream bin
          = new BufferedInputStream(new FileInputStream(f));
        Checksum checksum = FSEditLog.getChecksum();
        DataInputStream in
          = new DataInputStream(new CheckedInputStream(bin, checksum));

        FSEditLogLoader loader = new FSEditLogLoader();
        try {
          logVersion = loader.readLogVersion(in);
          startTxId = Long.valueOf(inProgressEditsMatch.group(1));
          FSEditLogOp.Reader reader = new FSEditLogOp.Reader(in, logVersion,
                                                             checksum);

          while (true) {
            FSEditLogOp op = reader.readOp();
            if (endTxId == -1) { // first transaction
              if (op.txid != startTxId) {
                corrupt = true;
                break;
              }
              endTxId = op.txid;
            } else if (op.txid == endTxId+1) {
              endTxId = op.txid;
            }
          }
        } catch (IOException ioe) {
          // reached end of file or incomplete transaction.
          // endTxId is the highest that can be read from this file
          LOG.info("Found end of log", ioe);
        } catch (NumberFormatException nfe) {
          LOG.error("In-progress edits file " + f + " has improperly " +
                    "formatted transaction ID");
          corrupt = true;
        } finally {
          in.close();
        }
        if (corrupt) {
          File src = f;
          File dst = new File(src.getParent(), src.getName() + ".corrupt");
          boolean success = src.renameTo(dst);
          if (!success) {
            LOG.error("Error moving corrupt file aside " + f);
          }
        } else {
          finalizeLogSegment(startTxId, endTxId);
        }
      }
    }
  }

  RemoteEditLogManifest getEditLogManifest(long fromTxId) throws IOException {
    List<RemoteEditLog> logs = new ArrayList<RemoteEditLog>();
    for (EditLogFile elf : getLogFiles(fromTxId)) {
      logs.add(new RemoteEditLog(elf.startTxId,
                                 elf.endTxId));
    }
    return new RemoteEditLogManifest(logs);
  }

  private List<EditLogFile> getLogFiles(long fromTxId) throws IOException {
    List<EditLogFile> logfiles = new ArrayList<EditLogFile>();
    File currentDir = sd.getCurrentDir();
    for (File f : currentDir.listFiles()) {
      Matcher editsMatch = EDITS_REGEX.matcher(f.getName());
      if (editsMatch.matches()) {
        long startTxId = Long.valueOf(editsMatch.group(1));
        long endTxId = Long.valueOf(editsMatch.group(2));

        if (fromTxId > startTxId
            && fromTxId <= endTxId) {
          throw new IOException("Asked for fromTxId " + fromTxId
                                + " which is in middle of file " + f);
        }
        if (fromTxId <= startTxId) {
          logfiles.add(new EditLogFile(startTxId, endTxId, f));
        }
      }
    }
    Collections.sort(logfiles, new Comparator<EditLogFile>() {
        public int compare(EditLogFile o1,
                           EditLogFile o2) {
          if (o1.startTxId < o2.startTxId) {
            return -1;
          } else if (o1.startTxId == o2.startTxId) {
            return 0;
          } else {
            return 1;
          }
        }
      });

    return logfiles;
  }

  private static class EditLogFile {
    final long startTxId;
    final long endTxId;
    final File file;

    EditLogFile(long startTxId, long endTxId, File file) {
      this.startTxId = startTxId;
      this.endTxId = endTxId;
      this.file = file;
    }
  }
}
