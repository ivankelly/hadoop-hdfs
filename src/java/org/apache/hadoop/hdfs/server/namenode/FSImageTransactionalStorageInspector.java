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

import java.net.URI;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;

import com.google.common.collect.Lists;

class FSImageTransactionalStorageInspector extends FSImageStorageInspector {
  public static final Log LOG = LogFactory.getLog(
    FSImageTransactionalStorageInspector.class);

  private boolean needToSave = false;
  private boolean isUpgradeFinalized = true;
  
  List<FoundFSImage> foundImages = new ArrayList<FoundFSImage>();
  
  private static final Pattern IMAGE_REGEX = Pattern.compile(
    NameNodeFile.IMAGE.getName() + "_(\\d+)");
  
  List<JournalManager> availableJournals = new ArrayList<JournalManager>();
  
  FSImageTransactionalStorageInspector(NNStorage storage) {
    super(storage);
  }

  @Override
  public void inspectImageDirectory(StorageDirectory sd) throws IOException {
    // Was the directory just formatted?
    if (!sd.getVersionFile().exists()) {
      LOG.info("No version file in " + sd.getRoot());
      needToSave |= true;
      return;
    }
    
    File currentDir = sd.getCurrentDir();

    for (File f : currentDir.listFiles()) {
      LOG.debug("Checking file " + f);
      String name = f.getName();
      
      // Check for fsimage_*
      Matcher imageMatch = IMAGE_REGEX.matcher(name);
      if (imageMatch.matches()) {
        if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
          try {
            long txid = Long.valueOf(imageMatch.group(1));
            foundImages.add(new FoundFSImage(sd, f, txid));
          } catch (NumberFormatException nfe) {
            LOG.error("Image file " + f + " has improperly formatted " +
                      "transaction ID");
            // skip
          }
        } else {
          LOG.warn("Found image file at " + f + " but storage directory is " +
                   "not configured to contain images.");
        }
      }
    }
      
    // set finalized flag
    isUpgradeFinalized = isUpgradeFinalized && !sd.getPreviousDir().exists();
  }

  @Override
  void inspectJournal(URI journalURI) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Inspecting journal " + journalURI);
    }
    if (journalURI.getScheme().equals("file")) {
      StorageDirectory sd = storage.getStorageDirectory(journalURI);
      if (sd != null) {
        availableJournals.add(new FileJournalManager(sd));
      } else {
        throw new IOException("Trying to use file storage "
                              + journalURI 
                              +" not managed by NNStorage.");
      }
    }
  }

  @Override
  public boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
  }
  
  /**
   * @return the image that has the most recent associated transaction ID.
   * If there are multiple storage directories which contain equal images 
   * the storage directory that was inspected first will be preferred.
   * 
   * Returns null if no images were found.
   */
  FoundFSImage getLatestImage() {
    FoundFSImage ret = null;
    for (FoundFSImage img : foundImages) {
      if (ret == null || img.txId > ret.txId) {
        ret = img;
      }
    }
    return ret;
  }

  @Override
  public LoadPlan createLoadPlan() throws IOException {
    if (foundImages.isEmpty()) {
      throw new FileNotFoundException("No valid image files found");
    }

    FoundFSImage recoveryImage = getLatestImage();
    long expectedTxId = recoveryImage.txId + 1;
    
    JournalManager bestjm = null;
    long mosttxn = 0;
    for (JournalManager jm : availableJournals) {
      try {
        long txncnt = jm.getNumberOfTransactions(expectedTxId);
        if (txncnt > 0 || bestjm == null) {
          bestjm = jm;
          mosttxn = txncnt;
        }
      } catch (IOException ioe) {
        LOG.error("Unable to get a transaction count from " + jm
                  + ". Will no use.", ioe);
      }
    }
    if (bestjm == null) {
      throw new IOException("No journal manager available");
    }

    return new TransactionalLoadPlan(recoveryImage, bestjm);
  }

  @Override
  public boolean needToSave() {
    return false; // TODO do we need to do this ever?
  }
  
  
  RemoteEditLogManifest getEditLogManifest(long sinceTxId) {
    List<RemoteEditLog> logs = Lists.newArrayList();
    /* IKTODO how to do this
    for (LogGroup g : logGroups.values()) {
      if (!g.hasFinalized) continue;

      FoundEditLog fel = g.getBestNonCorruptLog();
      if (fel.getLastTxId() < sinceTxId) continue;
      
      logs.add(new RemoteEditLog(fel.getStartTxId(),
          fel.getLastTxId()));
    }
    */
    return new RemoteEditLogManifest(logs);
  }

  /**
   * Record of an image that has been located and had its filename parsed.
   */
  static class FoundFSImage {
    final StorageDirectory sd;    
    final long txId;
    private final File file;
    
    FoundFSImage(StorageDirectory sd, File file, long txId) {
      assert txId >= 0 : "Invalid txid on " + file +": " + txId;
      
      this.sd = sd;
      this.txId = txId;
      this.file = file;
    } 
    
    File getFile() {
      return file;
    }

    public long getTxId() {
      return txId;
    }
  }
  
  static class TransactionalLoadPlan extends LoadPlan {
    final FoundFSImage image;
    final JournalManager jm;
    
    public TransactionalLoadPlan(FoundFSImage image, JournalManager jm) {
      super();
      this.image = image;
      this.jm = jm;
    }

    @Override
    boolean doRecovery() throws IOException {
      return false;
    }

    @Override
    File getImageFile() {
      return image.getFile();
    }

    @Override
    JournalManager getJournalManager() {
      return jm;
    }

    @Override
    StorageDirectory getStorageDirectoryForProperties() {
      return image.sd;
    }
  }
}
