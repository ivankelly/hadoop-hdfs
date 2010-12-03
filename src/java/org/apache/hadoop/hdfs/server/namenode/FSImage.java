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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Closeable;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirType;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.UpgradeManager;
import org.apache.hadoop.hdfs.server.common.Util;
import static org.apache.hadoop.hdfs.server.common.Util.now;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.StorageListener;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;

/**
 * FSImage handles checkpointing and logging of the namespace edits.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

public class FSImage implements StorageListener, Closeable {

  protected FSNamesystem namesystem = null;
  // TODELETE
  //protected long checkpointTime = -1L;  // The age of the image
  //protected FSEditLog editLog = null;
  private boolean isUpgradeFinalized = false;
  //protected MD5Hash imageDigest = null;
  //protected MD5Hash newImageDigest = null;

  /**
   * flag that controls if we try to restore failed storages
   */
  private boolean restoreFailedStorage = false;

  /**
   * list of failed (and thus removed) storages
   */
  //TODELETE
  //protected List<StorageDirectory> removedStorageDirs = new ArrayList<StorageDirectory>();
    
  /**
   * URIs for importing an image from a checkpoint. In the default case,
   * URIs will represent directories. 
   */
  //TODELETE
  //private Collection<URI> checkpointDirs;
  //private Collection<URI> checkpointEditsDirs;

  /**
   * Can fs-image be rolled?
   */
  //volatile protected CheckpointStates ckptState = FSImage.CheckpointStates.START; 

  protected static final Log LOG = LogFactory.getLog(FSImage.class.getName());
    
  protected Configuration conf;
  protected NNStorage storage;
  
  public FSImage(Configuration conf, NNStorage storage){
    assert conf != null && storage != null;

    this.conf = conf;
    this.storage = storage;
    
    this.storage.registerListener(this);

  }

  @Override
  public void errorOccurred(StorageDirectory sd) throws IOException {
    // do nothing,
  }

  @Override
  public void formatOccurred(StorageDirectory sd) throws IOException {
    if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
      sd.lock();
      try {
	saveCurrentImageToDirectory(sd);
      } finally {
	sd.unlock();
      }
      LOG.info("Storage directory " + sd.getRoot()
	       + " has been successfully formatted.");
    }
  };
  
  @Override
  public void directoryAvailable(StorageDirectory sd) throws IOException {
    // do nothing
  }
  
  protected FSNamesystem getFSNamesystem() {
    return namesystem;
  }

  public void setFSNamesystem(FSNamesystem ns) {
    namesystem = ns;
  }

  public void setRestoreFailedStorage(boolean val) {
    LOG.info("set restore failed storage to " + val);
    restoreFailedStorage=val;
  }
  
  public boolean getRestoreFailedStorage() {
    return restoreFailedStorage;
  }
  
  
  //FIXME: think on a better way to handle that.
  // setCheckpointDirectories is called from FSDirectory constructor
  // and from persistenceManager.load()
  public void setCheckpointDirectories(Collection<URI> dirs,
                                Collection<URI> editsDirs) {
   storage.setCheckpointDirectories(dirs, editsDirs);
  }
  
  public void upgrade() throws IOException {
    // Do upgrade for each directory
    long oldCTime = storage.getCTime();
    storage.cTime = now();  // generate new cTime for the state
    int oldLV = storage.getLayoutVersion();
    storage.layoutVersion = FSConstants.LAYOUT_VERSION;
    storage.checkpointTime = now();
    for (StorageDirectory sd : storage.iterable(NameNodeDirType.IMAGE)) {
      LOG.info("Upgrading image directory " + sd.getRoot()
               + ".\n   old LV = " + oldLV
               + "; old CTime = " + oldCTime
               + ".\n   new LV = " + storage.getLayoutVersion()
               + "; new CTime = " + storage.getCTime());
      File curDir = sd.getCurrentDir();
      File prevDir = sd.getPreviousDir();
      File tmpDir = sd.getPreviousTmp();
      assert curDir.exists() : "Current directory must exist.";
      assert !prevDir.exists() : "prvious directory must not exist.";
      assert !tmpDir.exists() : "prvious.tmp directory must not exist.";
      //assert !editLog.isOpen() : "Edits log must not be open.";
      // rename current to tmp
      storage.rename(curDir, tmpDir);
      // save new image
      saveCurrentImageToDirectory(sd);
      
      // rename tmp to previous
      storage.rename(tmpDir, prevDir);
      
      isUpgradeFinalized = false;
      LOG.info("Upgrade of " + sd.getRoot() + " is complete.");
    }
  }

  public void rollback() throws IOException {
    // Now that we know all directories are going to be consistent
    // Do rollback for each directory containing previous state
    for (StorageDirectory sd : storage) {
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists())
        continue;
      
      LOG.info("Rolling back storage directory " + sd.getRoot()
               + ".\n   new LV = " + storage.getLayoutVersion()
               + "; new CTime = " + storage.getCTime());
      File tmpDir = sd.getRemovedTmp();
      assert !tmpDir.exists() : "removed.tmp directory must not exist.";
      // rename current to tmp
      File curDir = sd.getCurrentDir();
      assert curDir.exists() : "Current directory must exist.";
      storage.rename(curDir, tmpDir);
      // rename previous to current
      storage.rename(prevDir, curDir);

      // delete tmp dir
      storage.deleteDir(tmpDir);
      LOG.info("Rollback of " + sd.getRoot()+ " is complete.");
    }
  }

  public void finalizeUpgrade() throws IOException {
    for (StorageDirectory sd : storage) {
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists()) { // already discarded
        LOG.info("Directory " + prevDir + " does not exist.");
        LOG.info("Finalize upgrade for " + sd.getRoot()+ " is not required.");
        return;
      }
      LOG.info("Finalizing upgrade for storage directory " 
               + sd.getRoot() + "."
               + (storage.getLayoutVersion()==0 ? "" :
                  "\n   cur LV = " + storage.getLayoutVersion()
                  + "; cur CTime = " + storage.getCTime()));
      assert sd.getCurrentDir().exists() : "Current directory must exist.";
      final File tmpDir = sd.getFinalizedTmp();
      // rename previous to tmp and remove
      storage.rename(prevDir, tmpDir);
      storage.deleteDir(tmpDir);
      LOG.info("Finalize upgrade for " + sd.getRoot()+ " is complete.");
    }
  }

  /**
   * @param sds - array of SDs to process
   * @param propagate - flag, if set - then call corresponding EditLog stream's 
   * processIOError function.
   */

  // FIXME : do we really still need this? why not error the directory directly
  void processIOError(List<StorageDirectory> sds)  throws IOException {
    synchronized (sds) {
	for(StorageDirectory sd:sds) {
	    storage.errorDirectory(sd);
	}
    }
  }

  /**
   * Choose latest image from one of the directories,
   * load it and merge with the edits from that directory.
   * 
   * Saving and loading fsimage should never trigger symlink resolution. 
   * The paths that are persisted do not have *intermediate* symlinks 
   * because intermediate symlinks are resolved at the time files, 
   * directories, and symlinks are created. All paths accessed while 
   * loading or saving fsimage should therefore only see symlinks as 
   * the final path component, and the functions called below do not
   * resolve symlinks that are the final path component.
   *
   * @return whether the image should be saved
   * @throws IOException
   */
  public NNStorage.LoadDirectory findLatestImageDirectory() throws IOException {
    long latestNameCheckpointTime = Long.MIN_VALUE;
    boolean needToSave = false;
    isUpgradeFinalized = true;
    
    StorageDirectory latestNameSD = null;
    
    Collection<String> imageDirs = new ArrayList<String>();
    
    // Set to determine if all of storageDirectories share the same checkpoint
    Set<Long> checkpointTimes = new HashSet<Long>();

    // Process each of the storage directories to find the pair of
    // newest image file and edit file
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();

      // Was the file just formatted?
      if (!sd.getVersionFile().exists()) {
        needToSave |= true;
        continue;
      }
      
      boolean imageExists = false;
      
      // Determine if sd is image, edits or both
      if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
        imageExists = storage.getImageFile(sd, NameNodeFile.IMAGE).exists();
        imageDirs.add(sd.getRoot().getCanonicalPath());
      }
      
      storage.checkpointTime = storage.readCheckpointTime(sd);

      checkpointTimes.add(storage.checkpointTime);
      
      if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE) && 
         (latestNameCheckpointTime < storage.checkpointTime) && imageExists) {
        latestNameCheckpointTime = storage.checkpointTime;
        latestNameSD = sd;
      }
      
      // check that we have a valid, non-default checkpointTime
      if (storage.checkpointTime <= 0L)
        needToSave |= true;
      
      // set finalized flag
      isUpgradeFinalized = isUpgradeFinalized && !sd.getPreviousDir().exists();
    }

    // We should have at least one image and one edits dirs
    if (latestNameSD == null)
      throw new IOException("Image file is not found in " + imageDirs);

    // If there was more than one checkpointTime recorded we should save
    needToSave |= checkpointTimes.size() != 1;

    return new NNStorage.LoadDirectory(latestNameSD, needToSave);
  }

  /**
   * Load in the filesystem image from file. It's a big list of
   * filenames and blocks.  Return whether we should
   * "re-save" and consolidate the edit-logs
   */
  public boolean loadFSImage(File curFile) throws IOException {
      FSImageFormat.Loader loader = new FSImageFormat.Loader(conf);
    loader.load(curFile, getFSNamesystem());


    // Check that the image digest we loaded matches up with what
    // we expected
    MD5Hash readImageMd5 = loader.getLoadedImageMd5();
    if (storage.imageDigest == null) {
      storage.imageDigest = readImageMd5; // set this fsimage's checksum
    } else if (!storage.imageDigest.equals(readImageMd5)) {
      throw new IOException("Image file " + curFile +
          " is corrupt with MD5 checksum of " + readImageMd5 +
          " but expecting " + storage.imageDigest);
    }

    storage.namespaceID = loader.getLoadedNamespaceID();
    storage.layoutVersion = loader.getLoadedImageVersion();

    boolean needToSave =
      loader.getLoadedImageVersion() != FSConstants.LAYOUT_VERSION;
    return needToSave;
  }

  /**
   * Save the contents of the FS image to the file.
   */
  void saveFSImage(File newFile) throws IOException {
    FSImageFormat.Writer writer = new FSImageFormat.Writer();
    FSImageCompression compression = FSImageCompression.createCompression(conf);
    writer.write(newFile, getFSNamesystem(), compression);
    storage.setImageDigest(writer.getWrittenDigest());
  }

  /**
   * FSImageSaver is being run in a separate thread when saving
   * FSImage. There is one thread per each copy of the image.
   *
   * FSImageSaver assumes that it was launched from a thread that holds
   * FSNamesystem lock and waits for the execution of FSImageSaver thread
   * to finish.
   * This way we are guraranteed that the namespace is not being updated
   * while multiple instances of FSImageSaver are traversing it
   * and writing it out.
   */
  private class FSImageSaver implements Runnable {
    private StorageDirectory sd;
    private List<StorageDirectory> errorSDs;
    
    FSImageSaver(StorageDirectory sd, List<StorageDirectory> errorSDs) {
      this.sd = sd;
      this.errorSDs = errorSDs;
    }
    
    public void run() {
      try {
        saveCurrentImageToDirectory(sd);
      } catch(IOException ie) {
        LOG.error("Unable to save image for " + sd.getRoot(), ie);
        errorSDs.add(sd);
      }
    }
    
    public String toString() {
      return "FSImageSaver for " + sd.getRoot() +
             " of type " + sd.getStorageDirType();
    }
  }
  
  private void waitForThreads(List<Thread> threads) {
    for (Thread thread : threads) {
      while (thread.isAlive()) {
        try {
          thread.join();
        } catch (InterruptedException iex) {
          LOG.error("Caught exception while waiting for thread " +
                    thread.getName() + " to finish. Retrying join");
        }        
      }
    }
  }
  /**
   * Save the contents of the FS image and create empty edits.
   * 
   * In order to minimize the recovery effort in case of failure during
   * saveNamespace the algorithm reduces discrepancy between directory states
   * by performing updates in the following order:
   * <ol>
   * <li> rename current to lastcheckpoint.tmp for all of them,</li>
   * <li> save image and recreate edits for all of them,</li>
   * <li> rename lastcheckpoint.tmp to previous.checkpoint.</li>
   * </ol>
   * On stage (2) we first save all images, then recreate edits.
   * Otherwise the name-node may purge all edits and fail,
   * in which case the journal will be lost.
   */
  public void saveNamespace(boolean renewCheckpointTime) throws IOException {
    if(renewCheckpointTime)
      storage.checkpointTime = now();

    List<StorageDirectory> errorSDs =
      Collections.synchronizedList(new ArrayList<StorageDirectory>());

    List<Thread> saveThreads = new ArrayList<Thread>();
    // save images into current
    for (Iterator<StorageDirectory> it = storage.dirIterator(NameNodeDirType.IMAGE);
                                                              it.hasNext();) {
      StorageDirectory sd = it.next();
    
      //INTRODUCED ON HDFS-1071, save images in parallel
      FSImageSaver saver = new FSImageSaver(sd, errorSDs);
      Thread saveThread = new Thread(saver, saver.toString());
      saveThreads.add(saveThread);
      saveThread.start();
    }
    waitForThreads(saveThreads);
    saveThreads.clear();

    // -NOTE-
    // If NN has image-only and edits-only storage directories and fails here 
    // the image will have the latest namespace state.
    // During startup the image-only directories will recover by discarding
    // lastcheckpoint.tmp, while
    // the edits-only directories will recover by falling back
    // to the old state contained in their lastcheckpoint.tmp.
    // The edits directories should be discarded during startup because their
    // checkpointTime is older than that of image directories.
    // recreate edits in current
  }

  /**
   * Save current image and empty journal into {@code current} directory.
   */
  public void saveCurrentImageToDirectory(StorageDirectory sd) throws IOException {
    File curDir = sd.getCurrentDir();
    NameNodeDirType dirType = (NameNodeDirType)sd.getStorageDirType();
    // save new image or new edits
    if (!curDir.exists() && !curDir.mkdir())
      throw new IOException("Cannot create directory " + curDir);
    if (dirType.isOfType(NameNodeDirType.IMAGE))
      saveFSImage(storage.getImageFile(sd, NameNodeFile.IMAGE));

    // write version and time files
    sd.write();
  }

  /** FIXME - delete
   * Return the name of the image file.
   */
  public File getFsImageName() {
    StorageDirectory sd = null;
    for (Iterator<StorageDirectory> it = 
      storage.dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      sd = it.next();
      if(sd.getRoot().canRead())
        return storage.getImageFile(sd, NameNodeFile.IMAGE); 
    }
    return null;
  }

  /** FIXME - DELETE
   * Return the name of the image file that is uploaded by periodic
   * checkpointing.
   */
  File[] getFsImageNameCheckpoint() {
    ArrayList<File> list = new ArrayList<File>();
    for (Iterator<StorageDirectory> it = 
                 storage.dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      list.add(storage.getImageFile(it.next(), NameNodeFile.IMAGE_NEW));
    }
    return list.toArray(new File[list.size()]);
  }

  /*
  private boolean getDistributedUpgradeState() {
    FSNamesystem ns = getFSNamesystem();
    return ns == null ? false : ns.getDistributedUpgradeState();
  }
  //TODELETE
  private int getDistributedUpgradeVersion() {
    FSNamesystem ns = getFSNamesystem();
    return ns == null ? 0 : ns.getDistributedUpgradeVersion();
  }
  //TODELETE
  private void setDistributedUpgradeState(boolean uState, int uVersion) {
    getFSNamesystem().upgradeManager.setUpgradeState(uState, uVersion);
  }
  */
  
  /**
   * Retrieve checkpoint dirs from configuration.
   *  
   * @param conf the Configuration
   * @param defaultValue a default value for the attribute, if null
   * @return a Collection of URIs representing the values in 
   * fs.checkpoint.dir configuration property
   */
  //TODELETE
  /*
  static Collection<URI> getCheckpointDirs(Configuration conf,
      String defaultValue) {
    Collection<String> dirNames = conf.getStringCollection(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);
    if (dirNames.size() == 0 && defaultValue != null) {
      dirNames.add(defaultValue);
    }
    return Util.stringCollectionAsURIs(dirNames);
  }
  */
  
  //TODELETE
  /*
  static Collection<URI> getCheckpointEditsDirs(Configuration conf,
      String defaultName) {
    Collection<String> dirNames = 
      conf.getStringCollection(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY);
    if (dirNames.size() == 0 && defaultName != null) {
      dirNames.add(defaultName);
    }
    return Util.stringCollectionAsURIs(dirNames);
  }  
  */

  public synchronized void close() throws IOException {
      // does nothing but may want to at some point
  }
}
