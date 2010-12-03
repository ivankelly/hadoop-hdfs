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

  // DELETEME
  // The filenames used for storing the images
  //
/*
   enum NameNodeFile {
    IMAGE     ("fsimage"),
    TIME      ("fstime"),
    EDITS     ("edits"),
    IMAGE_NEW ("fsimage.ckpt"),
    EDITS_NEW ("edits.new");
    
    private String fileName = null;
    private NameNodeFile(String name) {this.fileName = name;}
    String getName() {return fileName;}
  }
*/
  /**
   * Implementation of StorageDirType specific to namenode storage
   * A Storage directory could be of type IMAGE which stores only fsimage,
   * or of type EDITS which stores edits or of type IMAGE_AND_EDITS which 
   * stores both fsimage and edits.
   */
  /*  static enum NameNodeDirType implements StorageDirType {
    UNDEFINED,
    IMAGE,
    EDITS,
    IMAGE_AND_EDITS;
    
    public StorageDirType getStorageDirType() {
      return this;
    }
    
    public boolean isOfType(StorageDirType type) {
      if ((this == IMAGE_AND_EDITS) && (type == IMAGE || type == EDITS))
        return true;
      return this == type;
    }
    }*/

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

  /**
   */
  //TODELETE
  /*
  FSImage() {
    this((FSNamesystem)null);
  }
  */
  
  //TODELETE
  /*

  /**
   * Constructor
   * @param conf Configuration
   */

    /*
  FSImage(Configuration conf) throws IOException {
    this();
    this.conf = conf; // TODO we have too many constructors, this is a mess

    if(conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY, 
        DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_DEFAULT)) {
      NameNode.LOG.info("set FSImage.restoreFailedStorage");
      setRestoreFailedStorage(true);
    }
    setCheckpointDirectories(FSImage.getCheckpointDirs(conf, null),
        FSImage.getCheckpointEditsDirs(conf, null));
    this.compressImage = conf.getBoolean(
        DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY,
        DFSConfigKeys.DFS_IMAGE_COMPRESS_DEFAULT);
     this.codecFac = new CompressionCodecFactory(conf);
     if (this.compressImage) {
       String codecClassName = conf.get(
           DFSConfigKeys.DFS_IMAGE_COMPRESSION_CODEC_KEY,
           DFSConfigKeys.DFS_IMAGE_COMPRESSION_CODEC_DEFAULT);
       this.saveCodec = codecFac.getCodecByClassName(codecClassName);
       if (this.saveCodec == null) {
         throw new IOException("Not supported codec: " + codecClassName);
       }
     }
   }

  FSImage(FSNamesystem ns) {
    super(NodeType.NAME_NODE);
    //this.editLog = new FSEditLog(this);
    this.conf = new Configuration();
    setFSNamesystem(ns);
  }*/

  /**
   * @throws IOException 
   */
  //TODELETE
  /*
  FSImage(Collection<URI> fsDirs, Collection<URI> fsEditsDirs) 
      throws IOException {
    this();
    setStorageDirectories(fsDirs, fsEditsDirs);
  }*/

  //TODELETE
  /*
  public FSImage(StorageInfo storageInfo) {
    super(NodeType.NAME_NODE, storageInfo);
  }*/
  

  /**
   * Represents an Image (image and edit file).
   * @throws IOException 
   */
  //TODELETE
  /*
  FSImage(URI imageDir) throws IOException {
    this();
    ArrayList<URI> dirs = new ArrayList<URI>(1);
    ArrayList<URI> editsDirs = new ArrayList<URI>(1);
    dirs.add(imageDir);
    editsDirs.add(imageDir);
    setStorageDirectories(dirs, editsDirs);
  }
  */
  
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
  
  
  //TODELETE
  /*
  void setStorageDirectories(Collection<URI> fsNameDirs,
                             Collection<URI> fsEditsDirs) throws IOException {
    

    this.storageDirs = new ArrayList<StorageDirectory>();
    this.removedStorageDirs = new ArrayList<StorageDirectory>();
    
   // Add all name dirs with appropriate NameNodeDirType 
    for (URI dirName : fsNameDirs) {
      checkSchemeConsistency(dirName);
      boolean isAlsoEdits = false;
      for (URI editsDirName : fsEditsDirs) {
        if (editsDirName.compareTo(dirName) == 0) {
          isAlsoEdits = true;
          fsEditsDirs.remove(editsDirName);
          break;
        }
      }
      NameNodeDirType dirType = (isAlsoEdits) ?
                          NameNodeDirType.IMAGE_AND_EDITS :
                          NameNodeDirType.IMAGE;
      // Add to the list of storage directories, only if the 
      // URI is of type file://
      if(dirName.getScheme().compareTo(JournalType.FILE.name().toLowerCase()) 
          == 0){
        this.addStorageDir(new StorageDirectory(new File(dirName.getPath()), 
            dirType));
      }
    }
    
    // Add edits dirs if they are different from name dirs
    for (URI dirName : fsEditsDirs) {
      checkSchemeConsistency(dirName);
      // Add to the list of storage directories, only if the 
      // URI is of type file://
      if(dirName.getScheme().compareTo(JournalType.FILE.name().toLowerCase())
          == 0)
        this.addStorageDir(new StorageDirectory(new File(dirName.getPath()), 
                    NameNodeDirType.EDITS));
                    
    }
    
  }*/

  /* 
   * Checks the consistency of a URI, in particular if the scheme 
   * is specified and is supported by a concrete implementation 
   */
  //TODELETE
  /*
  static void checkSchemeConsistency(URI u) throws IOException {
    String scheme = u.getScheme();
    // the URI should have a proper scheme
    if(scheme == null)
      throw new IOException("Undefined scheme for " + u);
    else {
      try {
        // the scheme should be enumerated as JournalType
        JournalType.valueOf(scheme.toUpperCase());
      } catch (IllegalArgumentException iae){
        throw new IOException("Unknown scheme " + scheme + 
            ". It should correspond to a JournalType enumeration value");
      }
    }
  };
  */
  
  //FIXME: think on a better way to handle that.
  // setCheckpointDirectories is called from FSDirectory constructor
  // and from persistenceManager.load()
  public void setCheckpointDirectories(Collection<URI> dirs,
                                Collection<URI> editsDirs) {
   storage.setCheckpointDirectories(dirs, editsDirs);
  }
  
  
  
  static File getImageFile(StorageDirectory sd, NameNodeFile type) {
    return new File(sd.getCurrentDir(), type.getName());
  }
  
  
  
  //TODELETE
  /*
  File getEditFile(StorageDirectory sd) {
    return getImageFile(sd, NameNodeFile.EDITS);
  }*/
  
  //TODELETE
  /*
  File getEditNewFile(StorageDirectory sd) {
    return getImageFile(sd, NameNodeFile.EDITS_NEW);
  }*/

  
  
  // Used by checkpointStorage
  Collection<File> getFiles(NameNodeFile type, NameNodeDirType dirType) {
    // FIXME 
    return null;
  }
  

  Collection<URI> getDirectories(NameNodeDirType dirType) 
      throws IOException {
    ArrayList<URI> list = new ArrayList<URI>();
    Iterator<StorageDirectory> it = (dirType == null) ? storage.dirIterator() :
                                    storage.dirIterator(dirType);
    for ( ;it.hasNext(); ) {
      StorageDirectory sd = it.next();
      try {
        list.add(Util.fileAsURI(sd.getRoot()));
      } catch (IOException e) {
        throw new IOException("Exception while processing " +
            "StorageDirectory " + sd.getRoot(), e);
      }
    }
    return list;
  }

  /**
   * Retrieve current directories of type IMAGE
   * @return Collection of URI representing image directories 
   * @throws IOException in case of URI processing error
   */
  Collection<URI> getImageDirectories() throws IOException {
    return getDirectories(NameNodeDirType.IMAGE);
  }

  /**
   * Retrieve current directories of type EDITS
   * @return Collection of URI representing edits directories 
   * @throws IOException in case of URI processing error
   */
  Collection<URI> getEditsDirectories() throws IOException {
    return getDirectories(NameNodeDirType.EDITS);
  }

  /**
   * Return number of storage directories of the given type.
   * @param dirType directory type
   * @return number of storage directories of type dirType
   */
  //TODELETE
  /*
  int getNumStorageDirs(NameNodeDirType dirType) {
    if(dirType == null)
      return storage.getNumStorageDirs();
    Iterator<StorageDirectory> it = storage.dirIterator(dirType);
    int numDirs = 0;
    for(; it.hasNext(); it.next())
      numDirs++;
    return numDirs;
  }*/

  /**
   * Analyze storage directories.
   * Recover from previous transitions if required. 
   * Perform fs state transition if necessary depending on the namespace info.
   * Read storage info. 
   * 
   * @param dataDirs
   * @param startOpt startup option
   * @throws IOException
   * @return true if the image needs to be saved or false otherwise
   */
  /* TODELETE
    public boolean recoverTransitionRead(Collection<URI> dataDirs,
                                Collection<URI> editsDirs,
                                StartupOption startOpt)
      throws IOException {
    assert startOpt != StartupOption.FORMAT : 
      "NameNode formatting should be performed before reading the image";
    
    // none of the data dirs exist
    if((dataDirs.size() == 0 || editsDirs.size() == 0) 
                             && startOpt != StartupOption.IMPORT)  
      throw new IOException(
        "All specified directories are not accessible or do not exist.");
    
    if(startOpt == StartupOption.IMPORT 
        && (checkpointDirs == null || checkpointDirs.isEmpty()))
      throw new IOException("Cannot import image from a checkpoint. "
                          + "\"dfs.namenode.checkpoint.dir\" is not set." );

    if(startOpt == StartupOption.IMPORT 
        && (checkpointEditsDirs == null || checkpointEditsDirs.isEmpty()))
      throw new IOException("Cannot import image from a checkpoint. "
                          + "\"dfs.namenode.checkpoint.dir\" is not set." );
    
    setStorageDirectories(dataDirs, editsDirs);
    // 1. For each data directory calculate its state and 
    // check whether all is consistent before transitioning.
    Map<StorageDirectory, StorageState> dataDirStates = 
             new HashMap<StorageDirectory, StorageState>();
    boolean isFormatted = false;
    for (Iterator<StorageDirectory> it = 
                      storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      StorageState curState;
      try {
        curState = sd.analyzeStorage(startOpt);
        // sd is locked but not opened
        switch(curState) {
        case NON_EXISTENT:
          // name-node fails if any of the configured storage dirs are missing
          throw new InconsistentFSStateException(sd.getRoot(),
                      "storage directory does not exist or is not accessible.");
        case NOT_FORMATTED:
          break;
        case NORMAL:
          break;
        default:  // recovery is possible
          sd.doRecover(curState);      
        }
        if (curState != StorageState.NOT_FORMATTED 
            && startOpt != StartupOption.ROLLBACK) {
          sd.read(); // read and verify consistency with other directories
          isFormatted = true;
        }
        if (startOpt == StartupOption.IMPORT && isFormatted)
          // import of a checkpoint is allowed only into empty image directories
          throw new IOException("Cannot import image from a checkpoint. " 
              + " NameNode already contains an image in " + sd.getRoot());
      } catch (IOException ioe) {
        sd.unlock();
        throw ioe;
      }
      dataDirStates.put(sd,curState);
    }
    
    if (!isFormatted && startOpt != StartupOption.ROLLBACK 
                     && startOpt != StartupOption.IMPORT)
      throw new IOException("NameNode is not formatted.");
    if (layoutVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION) {
      checkVersionUpgradable(layoutVersion);
    }
    if (startOpt != StartupOption.UPGRADE
          && layoutVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION
          && layoutVersion != FSConstants.LAYOUT_VERSION)
        throw new IOException(
           "\nFile system image contains an old layout version " + layoutVersion
         + ".\nAn upgrade to version " + FSConstants.LAYOUT_VERSION
         + " is required.\nPlease restart NameNode with -upgrade option.");
    // check whether distributed upgrade is reguired and/or should be continued
    //verifyDistributedUpgradeProgress(startOpt);

    // 2. Format unformatted dirs.
    this.checkpointTime = 0L;
    for (Iterator<StorageDirectory> it = 
                     storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      StorageState curState = dataDirStates.get(sd);
      switch(curState) {
      case NON_EXISTENT:
        throw new IOException(StorageState.NON_EXISTENT + 
                              " state cannot be here");
      case NOT_FORMATTED:
        LOG.info("Storage directory " + sd.getRoot() + " is not formatted.");
        LOG.info("Formatting ...");
        sd.clearDirectory(); // create empty currrent dir
        break;
      default:
        break;
      }
    }

    // 3. Do transitions
    switch(startOpt) {
    case UPGRADE:
      doUpgrade();
      return false; // upgrade saved image already
    case IMPORT:
      doImportCheckpoint();
      return false; // import checkpoint saved image already
    case ROLLBACK:
      doRollback();
      break;
    case REGULAR:
      // just load the image
    }
    
    boolean needToSave = loadFSImage();
    
  
    assert editLog != null : "editLog must be initialized";
    if(!editLog.isOpen())
      editLog.open();
  
    return needToSave;
  }*/

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

  /*
  public void doUpgrade() throws IOException {
       
   

    if(getDistributedUpgradeState()) {
      // only distributed upgrade need to continue
      // don't do version upgrade
      this.loadFSImage();
      initializeDistributedUpgrade();
      return;
    }
    // Upgrade is allowed only if there are 
    // no previous fs states in any of the directories
    for (Iterator<StorageDirectory> it = 
                           storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      if (sd.getPreviousDir().exists())
        throw new InconsistentFSStateException(sd.getRoot(),
                                               "previous fs state should not exist during upgrade. "
                                               + "Finalize or rollback first.");
    }

    // load the latest image
    this.loadFSImage();

    
    storage.doUpgrade();
    
    // Do upgrade for each directory
    long oldCTime = this.getCTime();
    this.cTime = now();  // generate new cTime for the state
    int oldLV = this.getLayoutVersion();
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    this.checkpointTime = now();
    for (Iterator<StorageDirectory> it = 
                           storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      LOG.info("Upgrading image directory " + sd.getRoot()
               + ".\n   old LV = " + oldLV
               + "; old CTime = " + oldCTime
               + ".\n   new LV = " + this.getLayoutVersion()
               + "; new CTime = " + this.getCTime());
      File curDir = sd.getCurrentDir();
      File prevDir = sd.getPreviousDir();
      File tmpDir = sd.getPreviousTmp();
      assert curDir.exists() : "Current directory must exist.";
      assert !prevDir.exists() : "prvious directory must not exist.";
      assert !tmpDir.exists() : "prvious.tmp directory must not exist.";
      //assert !editLog.isOpen() : "Edits log must not be open.";
      // rename current to tmp
      rename(curDir, tmpDir);
      // save new image
      storage.saveCurrent(sd);
      // rename tmp to previous
      rename(tmpDir, prevDir);
      isUpgradeFinalized = false;
      LOG.info("Upgrade of " + sd.getRoot(upgrade) + " is complete.");
    }
    initializeDistributedUpgrade();
    //editLog.open();
    
    
    
    
    
  }
  */
  
  public void doRollback() throws IOException {
    
      //storage.doRollback();
    
    /* Move somewhere else
    // Rollback is allowed only if there is 
    // a previous fs states in at least one of the storage directories.
    // Directories that don't have previous state do not rollback
    boolean canRollback = false;
    FSImage prevState = new FSImage(getFSNamesystem());
    prevState.layoutVersion = FSConstants.LAYOUT_VERSION;
    for (Iterator<StorageDirectory> it = 
                       storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists()) {  // use current directory then
        LOG.info("Storage directory " + sd.getRoot()
                 + " does not contain previous fs state.");
        sd.read(); // read and verify consistency with other directories
        continue;
      }
      StorageDirectory sdPrev = prevState.new StorageDirectory(sd.getRoot());
      sdPrev.read(sdPrev.getPreviousVersionFile());  // read and verify consistency of the prev dir
      canRollback = true;
    }
    if (!canRollback)
      throw new IOException("Cannot rollback. " 
                            + "None of the storage directories contain previous fs state.");

    // Now that we know all directories are going to be consistent
    // Do rollback for each directory containing previous state
    for (Iterator<StorageDirectory> it = 
                          storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists())
        continue;

      LOG.info("Rolling back storage directory " + sd.getRoot()
               + ".\n   new LV = " + prevState.getLayoutVersion()
               + "; new CTime = " + prevState.getCTime());
      File tmpDir = sd.getRemovedTmp();
      assert !tmpDir.exists() : "removed.tmp directory must not exist.";
      // rename current to tmp
      File curDir = sd.getCurrentDir();
      assert curDir.exists() : "Current directory must exist.";
      rename(curDir, tmpDir);
      // rename previous to current
      rename(prevDir, curDir);

      // delete tmp dir
      deleteDir(tmpDir);
      LOG.info("Rollback of " + sd.getRoot()+ " is complete.");
    }
    isUpgradeFinalized = true;
    // check whether name-node can start in regular mode
    //verifyDistributedUpgradeProgress(StartupOption.REGULAR);
    */

  }

  //TODELETE
  /*
  private void doFinalize(StorageDirectory sd) throws IOException {
    // move somewhere else
    File prevDir = sd.getPreviousDir();
    if (!prevDir.exists()) { // already discarded
      LOG.info("Directory " + prevDir + " does not exist.");
      LOG.info("Finalize upgrade for " + sd.getRoot()+ " is not required.");
      return;
    }
    LOG.info("Finalizing upgrade for storage directory " 
             + sd.getRoot() + "."
             + (getLayoutVersion()==0 ? "" :
                   "\n   cur LV = " + this.getLayoutVersion()
                   + "; cur CTime = " + this.getCTime()));
    assert sd.getCurrentDir().exists() : "Current directory must exist.";
    final File tmpDir = sd.getFinalizedTmp();
    // rename previous to tmp and remove
    rename(prevDir, tmpDir);
    deleteDir(tmpDir);
    isUpgradeFinalized = true;
    LOG.info("Finalize upgrade for " + sd.getRoot()+ " is complete.");
   
  } */


  boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
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

  
  /*  public FSEditLog getEditLog() {
    return editLog;
    }*/
  

  // HDFS-259 RENAMES THIS METHOD TO isPreUpgradableLayout
  // TODELETE
  /*
  public boolean isConversionNeeded(StorageDirectory sd) throws IOException {
    File oldImageDir = new File(sd.getRoot(), "image");
    if (!oldImageDir.exists()) {
      return false;
    }
    // check the layout version inside the image file
    File oldF = new File(oldImageDir, "fsimage");
    RandomAccessFile oldFile = new RandomAccessFile(oldF, "rws");
    try {
      oldFile.seek(0);
      int odlVersion = oldFile.readInt();
      if (odlVersion < LAST_PRE_UPGRADE_LAYOUT_VERSION)
        return false;
    } finally {
      oldFile.close();
    }
    return true;

  }*/
 


  //
  // TODELETE
  // Atomic move sequence, to recover from interrupted checkpoint
  //
  //TODELETE
  /*
  boolean recoverInterruptedCheckpoint(StorageDirectory nameSD,
                                       StorageDirectory editsSD) 
                                       throws IOException {
    boolean needToSave = false;
    File curFile = getImageFile(nameSD, NameNodeFile.IMAGE);
    File ckptFile = getImageFile(nameSD, NameNodeFile.IMAGE_NEW);

    //
    // If we were in the midst of a checkpoint
    //
    if (ckptFile.exists()) {
      needToSave = true;
      if (getImageFile(editsSD, NameNodeFile.EDITS_NEW).exists()) {
        //
        // checkpointing migth have uploaded a new
        // merged image, but we discard it here because we are
        // not sure whether the entire merged image was uploaded
        // before the namenode crashed.
        //
        if (!ckptFile.delete()) {
          throw new IOException("Unable to delete " + ckptFile);
        }
      } else {
        //
        // checkpointing was in progress when the namenode
        // shutdown. The fsimage.ckpt was created and the edits.new
        // file was moved to edits. We complete that checkpoint by
        // moving fsimage.new to fsimage. There is no need to 
        // update the fstime file here. renameTo fails on Windows
        // if the destination file already exists.
        //
        if (!ckptFile.renameTo(curFile)) {
          if (!curFile.delete())
            LOG.warn("Unable to delete dir " + curFile + " before rename");
          if (!ckptFile.renameTo(curFile)) {
            throw new IOException("Unable to rename " + ckptFile +
                                  " to " + curFile);
          }
        }
      }
    }
    return needToSave;
  }*/

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
  // TODELETE
  /*  public boolean DELETETHISMETHODloadFSImage() throws IOException {
    long latestNameCheckpointTime = Long.MIN_VALUE;
    long latestEditsCheckpointTime = Long.MIN_VALUE;
    boolean needToSave = false;
    isUpgradeFinalized = true;
    
    StorageDirectory latestNameSD = null;
    StorageDirectory latestEditsSD = null;
    
    Collection<String> imageDirs = new ArrayList<String>();
    Collection<String> editsDirs = new ArrayList<String>();
    
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
      boolean editsExists = false;
      
      // Determine if sd is image, edits or both
      if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
        imageExists = getImageFile(sd, NameNodeFile.IMAGE).exists();
        imageDirs.add(sd.getRoot().getCanonicalPath());
      }
      
      if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
        editsExists = getImageFile(sd, NameNodeFile.EDITS).exists();
        editsDirs.add(sd.getRoot().getCanonicalPath());
      }
      
      checkpointTime = readCheckpointTime(sd);

      checkpointTimes.add(checkpointTime);
      
      if (sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE) && 
         (latestNameCheckpointTime < checkpointTime) && imageExists) {
        latestNameCheckpointTime = checkpointTime;
        latestNameSD = sd;
      }
      
      if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS) && 
           (latestEditsCheckpointTime < checkpointTime) && editsExists) {
        latestEditsCheckpointTime = checkpointTime;
        latestEditsSD = sd;
      }
      
      // check that we have a valid, non-default checkpointTime
      if (checkpointTime <= 0L)
        needToSave |= true;
      
      // set finalized flag
      isUpgradeFinalized = isUpgradeFinalized && !sd.getPreviousDir().exists();
    }

    // We should have at least one image and one edits dirs
    if (latestNameSD == null)
      throw new IOException("Image file is not found in " + imageDirs);
    if (latestEditsSD == null)
      throw new IOException("Edits file is not found in " + editsDirs);

    // Make sure we are loading image and edits from same checkpoint
    if (latestNameCheckpointTime > latestEditsCheckpointTime
        && latestNameSD != latestEditsSD
        && latestNameSD.getStorageDirType() == NameNodeDirType.IMAGE
        && latestEditsSD.getStorageDirType() == NameNodeDirType.EDITS) {
      // This is a rare failure when NN has image-only and edits-only
      // storage directories, and fails right after saving images,
      // in some of the storage directories, but before purging edits.
      // See -NOTE- in saveNamespace().
      LOG.error("This is a rare failure scenario!!!");
      LOG.error("Image checkpoint time " + latestNameCheckpointTime +
                " > edits checkpoint time " + latestEditsCheckpointTime);
      LOG.error("Name-node will treat the image as the latest state of " +
                "the namespace. Old edits will be discarded.");
    } else if (latestNameCheckpointTime != latestEditsCheckpointTime)
      throw new IOException("Inconsistent storage detected, " +
                      "image and edits checkpoint times do not match. " +
                      "image checkpoint time = " + latestNameCheckpointTime +
                      "edits checkpoint time = " + latestEditsCheckpointTime);
    
    // If there was more than one checkpointTime recorded we should save
    needToSave |= checkpointTimes.size() != 1;
    
    // Recover from previous interrupted checkpoint, if any
    needToSave |= recoverInterruptedCheckpoint(latestNameSD, latestEditsSD);

    //
    // Load in bits
    //
    latestNameSD.read();
    needToSave |= loadFSImage(getImageFile(latestNameSD, NameNodeFile.IMAGE));
    
    // Load latest edits
    if (latestNameCheckpointTime > latestEditsCheckpointTime)
      // the image is already current, discard edits
      needToSave |= true;
    else // latestNameCheckpointTime == latestEditsCheckpointTime
      needToSave |= (loadFSEdits(latestEditsSD) > 0);
    
    return needToSave;
    }*/

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
        imageExists = getImageFile(sd, NameNodeFile.IMAGE).exists();
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
    
  /*
    //HDFS-1473
  public boolean loadFSImage(File curFile) throws IOException {
    assert storage.getLayoutVersion() < 0 : "Negative layout version is expected.";
    assert curFile != null : "curFile is null";

    long startTime = now();   
    FSNamesystem fsNamesys = getFSNamesystem();
    FSDirectory fsDir = fsNamesys.dir;

    //
    // Load in bits
    //
    boolean needToSave = true;
    MessageDigest digester = MD5Hash.getDigester();
    DigestInputStream fin = new DigestInputStream(
         new FileInputStream(curFile), digester);

    DataInputStream in = new DataInputStream(fin);
    try {
      /*
       * Note: Remove any checks for version earlier than 
       * Storage.LAST_UPGRADABLE_LAYOUT_VERSION since we should never get 
       * to here with older images.
       *
      
      /*
       * TODO we need to change format of the image file
       * it should not contain version and namespace fields
       *
      // read image version: first appeared in version -1
      int imgVersion = in.readInt();
      needToSave = (imgVersion != FSConstants.LAYOUT_VERSION);

      // read namespaceID: first appeared in version -2
      //this.namespaceID = in.readInt();
      storage.setNamespaceId(in.readInt());

      // read number of files
      long numFiles;
      if (imgVersion <= -16) {
        numFiles = in.readLong();
      } else {
        numFiles = in.readInt();
      }

      //this.layoutVersion = imgVersion;
      storage.setLayoutVersion(imgVersion);
      
      // read in the last generation stamp.
      if (imgVersion <= -12) {
        long genstamp = in.readLong();
        fsNamesys.setGenerationStamp(genstamp); 
      }

      // read compression related info
      boolean isCompressed = false;
      if (imgVersion <= -25) {  // -25: 1st version providing compression option
        isCompressed = in.readBoolean();
        if (isCompressed) {
          String codecClassName = Text.readString(in);
          CompressionCodec loadCodec = codecFac.getCodecByClassName(codecClassName);
          if (loadCodec == null) {
            throw new IOException("Image compression codec not supported: "
                                 + codecClassName);
          }
          in = new DataInputStream(loadCodec.createInputStream(fin));
          LOG.info("Loading image file " + curFile +
              " compressed using codec " + codecClassName);
        }
      }
      if (!isCompressed) {
        // use buffered input stream
        in = new DataInputStream(new BufferedInputStream(fin));
      }
      
      // read file info
      short replication = fsNamesys.getDefaultReplication();

      LOG.info("Number of files = " + numFiles);

      byte[][] pathComponents;
      byte[][] parentPath = {{}};
      INodeDirectory parentINode = fsDir.rootDir;
      for (long i = 0; i < numFiles; i++) {
        long modificationTime = 0;
        long atime = 0;
        long blockSize = 0;
        pathComponents = readPathComponents(in);
        replication = in.readShort();
        replication = storage.adjustReplication(replication);
        modificationTime = in.readLong();
        if (imgVersion <= -17) {
          atime = in.readLong();
        }
        if (imgVersion <= -8) {
          blockSize = in.readLong();
        }
        int numBlocks = in.readInt();
        Block blocks[] = null;

        // for older versions, a blocklist of size 0
        // indicates a directory.
        if ((-9 <= imgVersion && numBlocks > 0) ||
            (imgVersion < -9 && numBlocks >= 0)) {
          blocks = new Block[numBlocks];
          for (int j = 0; j < numBlocks; j++) {
            blocks[j] = new Block();
            if (-14 < imgVersion) {
              blocks[j].set(in.readLong(), in.readLong(), 
                            GenerationStamp.GRANDFATHER_GENERATION_STAMP);
            } else {
              blocks[j].readFields(in);
            }
          }
        }
        // Older versions of HDFS does not store the block size in inode.
        // If the file has more than one block, use the size of the 
        // first block as the blocksize. Otherwise use the default block size.
        //
        if (-8 <= imgVersion && blockSize == 0) {
          if (numBlocks > 1) {
            blockSize = blocks[0].getNumBytes();
          } else {
            long first = ((numBlocks == 1) ? blocks[0].getNumBytes(): 0);
            blockSize = Math.max(fsNamesys.getDefaultBlockSize(), first);
          }
        }
        
        // get quota only when the node is a directory
        long nsQuota = -1L;
        if (imgVersion <= -16 && blocks == null  && numBlocks == -1) {
          nsQuota = in.readLong();
        }
        long dsQuota = -1L;
        if (imgVersion <= -18 && blocks == null && numBlocks == -1) {
          dsQuota = in.readLong();
        }
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

   * Return string representing the parent of the given path.
   */
  //TODELETE
  /*
  String getParent(String path) {
    return path.substring(0, path.lastIndexOf(Path.SEPARATOR));
  }*/

    //TODELETE
    /////////////////////////////////////////////
    //HDFS-1473 removes these
    /*
  byte[][] getParent(byte[][] path) {
    byte[][] result = new byte[path.length - 1][];
    for (int i = 0; i < result.length; i++) {
      result[i] = new byte[path[i].length];
      System.arraycopy(path[i], 0, result[i], 0, path[i].length);
    }
    return result;
  }

  private boolean isRoot(byte[][] path) {
    return path.length == 1 &&
      path[0] == null;    
  }

  private boolean isParent(byte[][] path, byte[][] parent) {
    if (path == null || parent == null)
      return false;
    if (parent.length == 0 || path.length != parent.length + 1)
      return false;
    boolean isParent = true;
    for (int i = 0; i < parent.length; i++) {
      isParent = isParent && Arrays.equals(path[i], parent[i]); 
    }
    return isParent;
    }*/
/////////////////////////////////////////////

  /**
   * Load and merge edits from two edits files
   * 
   * @param sd storage directory
   * @return number of edits loaded
   * @throws IOException
   */
  
  /*  DELETEME
int DELETEMEloadFSEdits(StorageDirectory sd) throws IOException {
    int numEdits = 0;
    EditLogFileInputStream edits = 
      new EditLogFileInputStream(getImageFile(sd, NameNodeFile.EDITS));
    
    numEdits = loader.loadFSEdits(edits);
    edits.close();
    File editsNew = getImageFile(sd, NameNodeFile.EDITS_NEW);
    
    if (editsNew.exists() && editsNew.length() > 0) {
      edits = new EditLogFileInputStream(editsNew);
      numEdits += loader.loadFSEdits(edits);
      edits.close();
    }
    
    // update the counts.
    getFSNamesystem().dir.updateCountForINodeWithQuota();    
    
    return numEdits;
    }*/

  /**
   * Save the contents of the FS image to the file.
   */
 
  void saveFSImage(File newFile) throws IOException {
  
  /*
    FSNamesystem fsNamesys = getFSNamesystem();
    FSDirectory fsDir = fsNamesys.dir;
    long startTime = now();
    //
    // Write out data
    //
    FileOutputStream fout = new FileOutputStream(newFile);
    MessageDigest digester = MD5Hash.getDigester();
    DigestOutputStream fos = new DigestOutputStream(fout, digester);

    DataOutputStream out = new DataOutputStream(fos);

    try {
      out.writeInt(FSConstants.LAYOUT_VERSION);
      out.writeInt(storage.getNamespaceID());
      out.writeLong(fsDir.rootDir.numItemsInTree());
      out.writeLong(fsNamesys.getGenerationStamp());
      
      // write compression info
      out.writeBoolean(compressImage);
      if (compressImage) {
        String codecClassName = saveCodec.getClass().getCanonicalName();
        Text.writeString(out, codecClassName);
        out = new DataOutputStream(saveCodec.createOutputStream(fos));
        LOG.info("Saving image file " + newFile +
            " compressed using codec " + codecClassName);
      } else {
        // use a buffered output stream
        out = new DataOutputStream(new BufferedOutputStream(fos));
      }

      byte[] byteStore = new byte[4*FSConstants.MAX_PATH_LENGTH];
      ByteBuffer strbuf = ByteBuffer.wrap(byteStore);
      // save the root
      saveINode2Image(strbuf, fsDir.rootDir, out);
      // save the rest of the nodes
      saveImage(strbuf, 0, fsDir.rootDir, out);
      fsNamesys.saveFilesUnderConstruction(out);
      fsNamesys.saveSecretManagerState(out);
      strbuf = null;

      out.flush();
      fout.getChannel().force(true);
    } finally {
      out.close();
    }

    // set md5 of the saved image
    setImageDigest( new MD5Hash(digester.digest()));

    LOG.info("Image file of size " + newFile.length() + " saved in " 
        + (now() - startTime)/1000 + " seconds.");
    
  }*/

    FSImageFormat.Writer writer = new FSImageFormat.Writer();
    FSImageCompression compression = FSImageCompression.createCompression(conf);
    writer.write(newFile, getFSNamesystem(), compression);
    storage.setImageDigest(writer.getWrittenDigest());
  }


  //public void setImageDigest(MD5Hash digest) {
  //   this.imageDigest = digest;
  // }
  
  
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
	  //saveCurrent(sd);
        saveCurrentImageToDirectory(sd);
        //saveFSImage(storage.getImageFile(sd, NameNodeFile.IMAGE));
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
    /* TODELETE
    assert editLog != null : "editLog must be initialized";
    editLog.close();
    */
    if(renewCheckpointTime)
      storage.checkpointTime = now();

    //ArrayList<StorageDirectory> errorSDs = new ArrayList<StorageDirectory>();
    List<StorageDirectory> errorSDs =
      Collections.synchronizedList(new ArrayList<StorageDirectory>());

    // mv current -> lastcheckpoint.tmp
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      try {
        storage.moveCurrent(sd);
      } catch(IOException ie) {
        LOG.error("Unable to move current for " + sd.getRoot(), ie);
        errorSDs.add(sd);
      }
    }

    List<Thread> saveThreads = new ArrayList<Thread>();
    // save images into current
    for (Iterator<StorageDirectory> it = storage.dirIterator(NameNodeDirType.IMAGE);
                                                              it.hasNext();) {
      StorageDirectory sd = it.next();
    
      /*
      try {

        //saveCurrent(sd);
        saveCurrentImageToDirectory(sd);
        //saveFSImage(storage.getImageFile(sd, NameNodeFile.IMAGE));
      } catch(IOException ie) {
        LOG.error("Unable to save image for " + sd.getRoot(), ie);
        errorSDs.add(sd);
	}*/
      
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
    /*
    for (Iterator<StorageDirectory> it = storage.dirIterator(NameNodeDirType.EDITS);
                                                              it.hasNext();) {
      StorageDirectory sd = it.next();
      /*
      //try {
      //  storage.saveCurrent(sd);
      //} catch(IOException ie) {
      //  LOG.error("Unable to save edits for " + sd.getRoot(), ie);
      //  errorSDs.add(sd);
      //}
    
      // Changed on HDFS-1073.. we got this piece of code commented
      final StorageDirectory sd = it.next();
      FSImageSaver saver = new FSImageSaver(sd, errorSDs);
      Thread saveThread = new Thread(saver, saver.toString());
      saveThreads.add(saveThread);
      saveThread.start();
    }
    waitForThreads(saveThreads);
    */


    /* TODELETE
    if(!editLog.isOpen()) editLog.open();
    */
  }

  /**
   * Generate new namespaceID.
   * 
   * namespaceID is a persistent attribute of the namespace.
   * It is generated when the namenode is formatted and remains the same
   * during the life cycle of the namenode.
   * When a datanodes register they receive it as the registrationID,
   * which is checked every time the datanode is communicating with the 
   * namenode. Datanodes that do not 'know' the namespaceID are rejected.
   * 
   * @return new namespaceID
   */
  // TODELETE
  /*
  private int newNamespaceID() {
    Random r = new Random();
    r.setSeed(now());
    int newID = 0;
    while(newID == 0)
      newID = r.nextInt(0x7FFFFFFF);  // use 31 bits only
    return newID;
  }
  */

  /** Create new dfs name directory.  Caution: this destroys all files
   * in this filesystem. */
  //TODELETE
  /*
  void format(StorageDirectory sd) throws IOException {
    
    sd.clearDirectory(); // create currrent dir
    sd.lock();
    try {
      storage.saveCurrent(sd);
    } finally {
      sd.unlock();
    }
    LOG.info("Storage directory " + sd.getRoot()
             + " has been successfully formatted.");
  }
  */

  public void storageFormatted() throws IOException {

    /*
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    this.namespaceID = newNamespaceID();
    this.cTime = 0L;
    this.checkpointTime = now();
    for (Iterator<StorageDirectory> it = 
                           storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      format(sd);
    }
    */
    
    /* FROM HDFS- trunk
    storage.setLayoutVersion(FSConstants.LAYOUT_VERSION);
    storage.setNamespaceId(storage.newNamespaceID());
    storage.setCTime(0L);
    storage.setCheckpointTime(now());
    */

    for (Iterator<StorageDirectory> it = storage.dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
        StorageDirectory sd = it.next();
        format(sd);
    }
  }

  /**
   * Format a device.
   * @param sd
   * @throws IOException
   */
  public void format(StorageDirectory sd) throws IOException {
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
      saveFSImage(getImageFile(sd, NameNodeFile.IMAGE));
    //if (dirType.isOfType(NameNodeDirType.EDITS))
      //editlog.createEditLogFile(getImageFile(sd, NameNodeFile.EDITS));
      //createEditLogFile(getImageFile(sd, NameNodeFile.EDITS));
    // write version and time files
    sd.write();
  }


  /** DELETEME
   * Moves fsimage.ckpt to fsImage and edits.new to edits
   * Reopens the new edits file.
   *
  void rollFSImage() throws IOException {
    rollFSImage(true);
    }*/


    //TODELETE
    //HDFS-903 checkpoint signature change
  /*
  void rollFSImage(CheckpointSignature sig, 
      boolean renewCheckpointTime) throws IOException {
    sig.validateStorageInfo(this);
    rollFSImage(true);
  }

  private void rollFSImage(boolean renewCheckpointTime)
  throws IOException {
    if (ckptState != CheckpointStates.UPLOAD_DONE
      && !(ckptState == CheckpointStates.ROLLED_EDITS
      && getNumStorageDirs(NameNodeDirType.IMAGE) == 0)) {
      throw new IOException("Cannot roll fsImage before rolling edits log.");
    }

    for (Iterator<StorageDirectory> it = 
                       dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      StorageDirectory sd = it.next();
      File ckpt = getImageFile(sd, NameNodeFile.IMAGE_NEW);
      if (!ckpt.exists()) {
        throw new IOException("Checkpoint file " + ckpt +
                              " does not exist");
      }
    }
    editLog.purgeEditLog(); // renamed edits.new to edits
    if(LOG.isDebugEnabled()) {
      LOG.debug("rollFSImage after purgeEditLog: storageList=" + listStorageDirectories());
    }
    //
    // Renames new image
    //
    renameCheckpoint();
    resetVersion(renewCheckpointTime, newImageDigest);
  }
  */
  
  /** FIXME - delete
   * Return the name of the image file.
   */
  public File getFsImageName() {
    StorageDirectory sd = null;
    for (Iterator<StorageDirectory> it = 
      storage.dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      sd = it.next();
      if(sd.getRoot().canRead())
        return getImageFile(sd, NameNodeFile.IMAGE); 
    }
    return null;
  }

  /**
   * See if any of removed storages iw "writable" again, and can be returned 
   * into service
   */
  // TO DELETE
  /*
  synchronized void attemptRestoreRemovedStorage() {   
    // if directory is "alive" - copy the images there...
    if(!restoreFailedStorage || removedStorageDirs.size() == 0) 
      return; //nothing to restore
    
    LOG.info("FSImage.attemptRestoreRemovedStorage: check removed(failed) " +
        "storarge. removedStorages size = " + removedStorageDirs.size());
    for(Iterator<StorageDirectory> it = this.removedStorageDirs.iterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File root = sd.getRoot();
      LOG.info("currently disabled dir " + root.getAbsolutePath() + 
          "; type="+sd.getStorageDirType() + ";canwrite="+root.canWrite());
      try {
        
        if(root.exists() && root.canWrite()) { 
          format(sd);
          LOG.info("restoring dir " + sd.getRoot().getAbsolutePath());
          if(sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
            File eFile = getEditFile(sd);
            editLog.addNewEditLogStream(eFile);
          }
          this.addStorageDir(sd); // restore
          it.remove();
        }
      } catch(IOException e) {
        LOG.warn("failed to restore " + sd.getRoot().getAbsolutePath(), e);
      }
    }    
  }
 
  public File getFsEditName() throws IOException {
    return getEditLog().getFsEditName();
  }
 */

  //TODELETE
  /*
  File getFsTimeName() {
    StorageDirectory sd = null;
    // NameNodeFile.TIME shoul be same on all directories
    for (Iterator<StorageDirectory> it = 
             storage.dirIterator(); it.hasNext();)
      sd = it.next();
    return getImageFile(sd, NameNodeFile.TIME);
  }*/

  /** FIXME - DELETE
   * Return the name of the image file that is uploaded by periodic
   * checkpointing.
   */
  File[] getFsImageNameCheckpoint() {
    ArrayList<File> list = new ArrayList<File>();
    for (Iterator<StorageDirectory> it = 
                 storage.dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      list.add(getImageFile(it.next(), NameNodeFile.IMAGE_NEW));
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
  
  //TODELETE
  /*
  private void verifyDistributedUpgradeProgress(StartupOption startOpt
                                                ) throws IOException {
    if(startOpt == StartupOption.ROLLBACK || startOpt == StartupOption.IMPORT)
      return;
    UpgradeManager um = getFSNamesystem().upgradeManager;
    assert um != null : "FSNameSystem.upgradeManager is null.";
    if(startOpt != StartupOption.UPGRADE) {
      if(um.getUpgradeState())
        throw new IOException(
                    "\n   Previous distributed upgrade was not completed. "
                  + "\n   Please restart NameNode with -upgrade option.");
      if(um.getDistributedUpgrades() != null)
        throw new IOException("\n   Distributed upgrade for NameNode version " 
          + um.getUpgradeVersion() + " to current LV " + FSConstants.LAYOUT_VERSION
          + " is required.\n   Please restart NameNode with -upgrade option.");
    }
  }
  */
  
  //TODELETE
  /*
  private void initializeDistributedUpgrade() throws IOException {
    UpgradeManagerNamenode um = getFSNamesystem().upgradeManager;
    if(! um.initializeUpgrade())
      return;
    // write new upgrade state into disk
    storage.writeAll();
    NameNode.LOG.info("\n   Distributed upgrade for NameNode version " 
        + um.getUpgradeVersion() + " to current LV " 
        + FSConstants.LAYOUT_VERSION + " is initialized.");
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
