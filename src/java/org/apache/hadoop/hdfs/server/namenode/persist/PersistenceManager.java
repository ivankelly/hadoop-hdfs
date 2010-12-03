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
package org.apache.hadoop.hdfs.server.namenode.persist;

import org.apache.hadoop.hdfs.server.common.Storage; //TODO remove when we have concrete
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.UpgradeManagerNamenode;

import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;

import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NNUtils;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile; 

import org.apache.hadoop.hdfs.DFSConfigKeys;
import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ArrayList;

import java.io.IOException;
import java.io.File;
import java.io.Closeable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hdfs.server.common.Storage; 
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory; 
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.UpgradeManager;

import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.protocol.FSConstants;

import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import org.apache.hadoop.io.MD5Hash;


//import org.apache.hadopp.hdfs.server.namenode.FSEditLogLoader;

public class PersistenceManager implements Closeable {
  public static final Log LOG = LogFactory.getLog(PersistenceManager.class.getName());

  private static final SimpleDateFormat DATE_FORM = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  protected Configuration conf;
  protected FSImage image;
  protected FSEditLog editlog;
  protected NNStorage storage;
  protected FSNamesystem namesystem;

  private boolean isUpgradeFinalized = false;
  

  /**
   * Can fs-image be rolled?
   */
  // checkpoint states
  public enum CheckpointStates{START, ROLLED_EDITS, UPLOAD_START, UPLOAD_DONE; }

  volatile protected CheckpointStates ckptState = CheckpointStates.START; 


  /* Constructor */
  protected PersistenceManager() {
    // dummy constructor for accessors
  }

  public PersistenceManager(Configuration conf, NNStorage storage) {
    this.conf = conf;
    this.storage = storage;
    namesystem = null;
    
    editlog = new FSEditLog(conf, storage);
    image = new FSImage(conf,storage);

    if(conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY, 
                       DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_DEFAULT)) {
      LOG.info("set FSImage.restoreFailedStorage");
      image.setRestoreFailedStorage(true);
    }
    
    image.setCheckpointDirectories(NNUtils.getCheckpointDirs(conf, null),
				   NNUtils.getCheckpointEditsDirs(conf, null));
  }

  public void setNamesystem(FSNamesystem namesystem) {
    this.namesystem = namesystem;
    image.setFSNamesystem(namesystem);
    editlog.setFSNamesystem(namesystem);
  }
  
  private boolean getDistributedUpgradeState() {
    return namesystem == null ? false : namesystem.getDistributedUpgradeState();
  }
  
  
  /**
   * Start checkpoint.
   * <p>
   * If backup storage contains image that is newer than or incompatible with 
   * what the active name-node has, then the backup node should shutdown.<br>
   * If the backup image is older than the active one then it should 
   * be discarded and downloaded from the active node.<br>
   * If the images are the same then the backup image will be used as current.
   * 
   * @param bnReg the backup node registration.
   * @param nnReg this (active) name-node registration.
   * @return {@link NamenodeCommand} if backup node should shutdown or
   * {@link CheckpointCommand} prescribing what backup node should 
   *         do with its image.
   * @throws IOException
   */
  public NamenodeCommand startCheckpoint(NamenodeRegistration bnReg, // backup node
					 NamenodeRegistration nnReg) // active name-node
  throws IOException {
    String msg = null;
    // Verify that checkpoint is allowed
    if(bnReg.getNamespaceID() != storage.getNamespaceID())
      msg = "Name node " + bnReg.getAddress()
            + " has incompatible namespace id: " + bnReg.getNamespaceID()
            + " expected: " + storage.getNamespaceID();
    else if(bnReg.isRole(NamenodeRole.ACTIVE))
      msg = "Name node " + bnReg.getAddress()
            + " role " + bnReg.getRole() + ": checkpoint is not allowed.";
    else if(bnReg.getLayoutVersion() < storage.getLayoutVersion()
        || (bnReg.getLayoutVersion() == storage.getLayoutVersion()
            && bnReg.getCTime() > storage.getCTime())
        || (bnReg.getLayoutVersion() == storage.getLayoutVersion()
            && bnReg.getCTime() == storage.getCTime()
            && bnReg.getCheckpointTime() > storage.getCheckpointTime()))
      // remote node has newer image age
      msg = "Name node " + bnReg.getAddress()
	+ " has newer image layout version: LV = " +bnReg.getLayoutVersion()
	+ " cTime = " + bnReg.getCTime()
	+ " checkpointTime = " + bnReg.getCheckpointTime()
	+ ". Current version: LV = " + storage.getLayoutVersion()
	+ " cTime = " + storage.getCTime()
	+ " checkpointTime = " + storage.getCheckpointTime();
    if(msg != null) {
      LOG.error(msg);
      return new NamenodeCommand(NamenodeProtocol.ACT_SHUTDOWN);
    }
    boolean isImgObsolete = true;
    if(bnReg.getLayoutVersion() == storage.getLayoutVersion()
       && bnReg.getCTime() == storage.getCTime()
       && bnReg.getCheckpointTime() == storage.getCheckpointTime())
      isImgObsolete = false;

    boolean needToReturnImg = true;
    if(storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0)
      // do not return image if there are no image directories
      needToReturnImg = false;
    CheckpointSignature sig = rollEditLog();
    editlog.logJSpoolStart(bnReg, nnReg);
    return new CheckpointCommand(sig, isImgObsolete, needToReturnImg);
  }


  /**
   * This is called just before a new checkpoint is uploaded to the
   * namenode.
   */
  public void validateCheckpointUpload(CheckpointSignature sig) throws IOException {
    if (ckptState != CheckpointStates.ROLLED_EDITS) {
      throw new IOException("Namenode is not expecting an new image " +
                             ckptState);
    } 
    // verify token
    long modtime = editlog.getFsEditTime();
    if (sig.editsTime != modtime) {
      throw new IOException("Namenode has an edit log with timestamp of " +
                            DATE_FORM.format(new Date(modtime)) +
                            " but new checkpoint was created using editlog " +
                            " with timestamp " + 
                            DATE_FORM.format(new Date(sig.editsTime)) + 
                            ". Checkpoint Aborted.");
    }
    // FIXME sig.validateStorageInfo(this);
    ckptState = CheckpointStates.UPLOAD_START;
  }


  public void rollFSImage(CheckpointSignature sig, 
      boolean renewCheckpointTime) throws IOException {
    sig.validateStorageInfo(this.storage);
    rollFSImage(true);
  }

  public void rollFSImage(boolean renewCheckpointTime) throws IOException {
    if (ckptState != CheckpointStates.UPLOAD_DONE
	&& !(ckptState == CheckpointStates.ROLLED_EDITS
	     && storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0)) {
      throw new IOException("Cannot roll fsImage before rolling edits log.");
    }
    
    for (StorageDirectory sd : storage.iterable(NameNodeDirType.IMAGE)) {
      File ckpt = storage.getImageFile(sd, NameNodeFile.IMAGE_NEW);
      if (!ckpt.exists()) {
        throw new IOException("Checkpoint file " + ckpt +
                              " does not exist");
      }
    }
    editlog.purgeEditLog(); // renamed edits.new to edits
    if(LOG.isDebugEnabled()) {
      LOG.debug("rollFSImage after purgeEditLog: storageList=" + storage.listStorageDirectories());
    }
    //
    // Renames new image
    //
    renameCheckpoint();

    resetVersion(renewCheckpointTime, storage.getNewImageDigest());
  }

  /**
   * Updates version and fstime files in all directories (fsimage and edits).
   */
    protected void resetVersion(boolean renewCheckpointTime, MD5Hash newImageDigest) throws IOException {
    storage.layoutVersion = FSConstants.LAYOUT_VERSION;
    if(renewCheckpointTime)
      storage.setCheckpointTime(now());
    
    storage.setImageDigest(newImageDigest);

    ArrayList<StorageDirectory> al = null;
    for (StorageDirectory sd : storage) {
      // delete old edits if sd is the image only the directory
      if (!sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
        File editsFile = storage.getImageFile(sd, NameNodeFile.EDITS);
        if(editsFile.exists() && !editsFile.delete())
          throw new IOException("Cannot delete edits file " 
                                + editsFile.getCanonicalPath());
      }
      // delete old fsimage if sd is the edits only the directory
      if (!sd.getStorageDirType().isOfType(NameNodeDirType.IMAGE)) {
        File imageFile = storage.getImageFile(sd, NameNodeFile.IMAGE);
        if(imageFile.exists() && !imageFile.delete())
          throw new IOException("Cannot delete image file " 
                                + imageFile.getCanonicalPath());
      }
      try {
        sd.write();
      } catch (IOException e) {
        LOG.error("Cannot write file " + sd.getRoot(), e);
        
        if(al == null) al = new ArrayList<StorageDirectory> (1);
        al.add(sd);       
      }
    }
    //  if(al != null) processIOError(al); FIXME make this call into the right place
    ckptState = CheckpointStates.START;
  }


  /**
   * End checkpoint.
   * <p>
   * Rename uploaded checkpoint to the new image;
   * purge old edits file;
   * rename edits.new to edits;
   * redirect edit log streams to the new edits;
   * update checkpoint time if the remote node is a checkpoint only node.
   * 
   * @param sig
   * @param remoteNNRole
   * @throws IOException
   */
  public void endCheckpoint(CheckpointSignature sig, 
                     NamenodeRole remoteNNRole) throws IOException {
      // FIXME sig.validateStorageInfo(this);
    // Renew checkpoint time for the active if the other is a checkpoint-node.
    // The checkpoint-node should have older image for the next checkpoint 
    // to take effect.
    // The backup-node always has up-to-date image and will have the same
    // checkpoint time as the active node.
    boolean renewCheckpointTime = remoteNNRole.equals(NamenodeRole.CHECKPOINT);
    rollFSImage(sig,renewCheckpointTime);
  }

  public CheckpointStates getCheckpointState() {
    return ckptState;
  }

  public void setCheckpointState(CheckpointStates cs) {
    ckptState = cs;
  }

  /**
   * This is called when a checkpoint upload finishes successfully.
   */
  public synchronized void checkpointUploadDone() {
    ckptState = CheckpointStates.UPLOAD_DONE;
  }


  /**
   * Renames new image
   */
  private void renameCheckpoint() {
    ArrayList<StorageDirectory> al = null;
    for (StorageDirectory sd : storage.iterable(NameNodeDirType.IMAGE)) {
      File ckpt = storage.getImageFile(sd, NameNodeFile.IMAGE_NEW);
      File curFile = storage.getImageFile(sd, NameNodeFile.IMAGE);
      // renameTo fails on Windows if the destination file 
      // already exists.
      if(LOG.isDebugEnabled()) {
        LOG.debug("renaming  " + ckpt.getAbsolutePath() + " to "  + curFile.getAbsolutePath());
      }
      if (!ckpt.renameTo(curFile)) {
        if (!curFile.delete() || !ckpt.renameTo(curFile)) {
          LOG.warn("renaming  " + ckpt.getAbsolutePath() + " to "  + 
              curFile.getAbsolutePath() + " FAILED");

          if(al == null) al = new ArrayList<StorageDirectory> (1);
          al.add(sd);
        }
      }
    }
    // FIXME pass the error properly somehow if(al != null) processIOError(al); 
  }


  public CheckpointSignature rollEditLog() throws IOException {
    editlog.rollEditLog();
    ckptState = CheckpointStates.ROLLED_EDITS;
    // If checkpoint fails this should be the most recent image, therefore
    storage.incrementCheckpointTime();
    //return new CheckpointSignature(null); //FIXME should pass NNStorage
    return null;
  }



  public void importCheckpoint() throws IOException {
    storage.initializeDirectories( getStartupOption() );

    /*
      This should create a new Storage and new Image. The directories should be set in the new Storage to reflect
      the checkpoint directories. 

    FSNamesystem fsNamesys = getFSNamesystem();
    FSImage ckptImage = new FSImage(fsNamesys);
    // replace real image with the checkpoint image
    FSImage realImage = fsNamesys.getFSImage();
    assert realImage == this;
    fsNamesys.dir.fsImage = ckptImage;
    // load from the checkpoint dirs
    try {
      ckptImage.recoverTransitionRead(checkpointDirs, checkpointEditsDirs,
                                              StartupOption.REGULAR);
    } finally {
      ckptImage.close();
    }
    // return back the real image
    realImage.setStorageInfo(ckptImage);
    checkpointTime = ckptImage.checkpointTime;
    fsNamesys.dir.fsImage = realImage;
    // and save it but keep the same checkpointTime
    saveNamespace(false);
    */


  }

  /**
     Implement functionallity used by the command line options
  */
  public void upgrade() throws IOException {
    if(getDistributedUpgradeState()) {
      // only distributed upgrade need to continue
      // don't do version upgrade
      this.load();
      initializeDistributedUpgrade();
      return;
    }
    
    this.load();

    for (StorageDirectory sd : storage) {
      if (sd.getPreviousDir().exists())
        throw new InconsistentFSStateException(sd.getRoot(),
					       "previous fs state should not exist during upgrade. "
					       + "Finalize or rollback first.");
    }

    editlog.close();
    storage.close();

    assert !editlog.isOpen();

    image.upgrade();
    editlog.upgrade();

    editlog.createEditLogFiles();
    editlog.open();

    isUpgradeFinalized = false;

    initializeDistributedUpgrade();
  }

  public void rollback() throws IOException {
    // Rollback is allowed only if there is 
    // a previous fs states in at least one of the storage directories.
    // Directories that don't have previous state do not rollback
    boolean canRollback = false;
    
    storage.layoutVersion = FSConstants.LAYOUT_VERSION;
    
    for (StorageDirectory sd : storage) {
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists()) {  // use current directory then
        LOG.info("Storage directory " + sd.getRoot()
                 + " does not contain previous fs state.");
        sd.read(); // read and verify consistency with other directories
        continue;
      }
      StorageDirectory sdPrev = storage.new StorageDirectory(sd.getRoot());
      sdPrev.read(sdPrev.getPreviousVersionFile());  // read and verify consistency of the prev dir
      canRollback = true;
    }
    if (!canRollback) {
      throw new IOException("Cannot rollback. " 
                            + "None of the storage directories contain previous fs state.");      
    }
    
    editlog.close();
    storage.close();
    assert !editlog.isOpen();

    image.rollback();
    editlog.rollback();
    
    isUpgradeFinalized = true;

    // check whether name-node can start in regular mode
    verifyDistributedUpgradeProgress(StartupOption.REGULAR);
  }
  
  private void initializeDistributedUpgrade() throws IOException {
    UpgradeManagerNamenode um = namesystem.getUpgradeManager();
    if(! um.initializeUpgrade())
      return;

    // write new upgrade state into disk
    storage.writeAll();
    NameNode.LOG.info("\n   Distributed upgrade for NameNode version " 
        + um.getUpgradeVersion() + " to current LV " 
        + FSConstants.LAYOUT_VERSION + " is initialized.");
  }
  
  private void verifyDistributedUpgradeProgress(StartupOption startOpt) throws IOException {
    if(startOpt == StartupOption.ROLLBACK || startOpt == StartupOption.IMPORT) {
      return;
    }
    
    UpgradeManager um = namesystem.upgradeManager;
    assert um != null : "FSNameSystem.upgradeManager is null.";
    if(startOpt != StartupOption.UPGRADE) {
      if(um.getUpgradeState())
        throw new IOException("\n   Previous distributed upgrade was not completed. "
                              + "\n   Please restart NameNode with -upgrade option.");
      if(um.getDistributedUpgrades() != null)
        throw new IOException("\n   Distributed upgrade for NameNode version " 
            + um.getUpgradeVersion() + " to current LV " + FSConstants.LAYOUT_VERSION
            + " is required.\n   Please restart NameNode with -upgrade option.");
    }
  }


  public  void finalizeUpgrade() throws IOException  {
    
  }

  /*  public void format() throws IOException { 
    storage.initializeDirectories( getStartupOption() );
    storage.format();
    }*/

  /**
     Save the contents of FSNameSystem to disk
     Fundamentally just dumps mem and resets the edit log
  */
  public void save() throws IOException {
    assert editlog != null : "editLog must be initialized";
    editlog.close();

    image.saveNamespace(true);
    editlog.createEditLogFiles();

    // mv lastcheckpoint.tmp -> previous.checkpoint
    for (StorageDirectory sd : storage) {
      try {
        storage.moveLastCheckpoint(sd);
      } catch(IOException ie) {
          LOG.error("Unable to move last checkpoint for " + sd.getRoot(), ie);
          storage.errorDirectory(sd);
      }
    }
    
    if(!editlog.isOpen()) {
      editlog.open();
    }
    
    ckptState = CheckpointStates.UPLOAD_DONE;
  }

  /**
     Load the latest version of the FSNameSystem from disk
     w Interfaces
     
     @return Whether the save should be called soon
  */
  public boolean load() throws IOException {
    boolean needToSave = false;

    try {
      // FIXME where do i get the dataDirs? editsDirs? should recoverTransitionRead even be a method
      storage.initializeDirectories( getStartupOption() );
      
      // check whether distributed upgrade is reguired and/or should be continued
      verifyDistributedUpgradeProgress( getStartupOption() );


      assert editlog != null : "editLog must be initialized";
      NNStorage.LoadDirectory imagedir = image.findLatestImageDirectory();
      NNStorage.LoadDirectory editsdir = editlog.findLatestEditsDirectory();

      needToSave |= imagedir.getNeedToSave() || editsdir.getNeedToSave();

      long imgtime = storage.readCheckpointTime(imagedir.getDirectory());
      long editstime = storage.readCheckpointTime(editsdir.getDirectory());

      // Make sure we are loading image and edits from same checkpoint
      if (imgtime > editstime
	  && imagedir.getDirectory() != editsdir.getDirectory()
	  && imagedir.getDirectory().getStorageDirType() == NameNodeDirType.IMAGE
	  && editsdir.getDirectory().getStorageDirType() == NameNodeDirType.EDITS) {
	// This is a rare failure when NN has image-only and edits-only
	// storage directories, and fails right after saving images,
	// in some of the storage directories, but before purging edits.
	// See -NOTE- in saveNamespace().
	LOG.error("This is a rare failure scenario!!!");
	LOG.error("Image checkpoint time " + imgtime +
		  " > edits checkpoint time " + editstime);
	LOG.error("Name-node will treat the image as the latest state of " +
		  "the namespace. Old edits will be discarded.");
      } else if (editstime != imgtime) {
	throw new IOException("Inconsistent storage detected, " +
			      "image and edits checkpoint times do not match. " +
			      "image checkpoint time = " + imgtime +
			      "edits checkpoint time = " + editstime);
      }
      
      // Recover from previous interrupted checkpoint, if any
      needToSave |= storage.recoverInterruptedCheckpoint(imagedir.getDirectory(), editsdir.getDirectory());
      
      long startTime = now();
      long imageSize = storage.getImageFile(imagedir.getDirectory(), NameNodeFile.IMAGE).length();
    
      //
      // Load in bits
      //
      imagedir.getDirectory().read();
      needToSave |= image.loadFSImage(storage.getImageFile(imagedir.getDirectory(), NameNodeFile.IMAGE));
      LOG.info("Image file of size " + imageSize + " loaded in " 
	       + (now() - startTime)/1000 + " seconds.");
    
      // Load latest edits
      if (imgtime > editstime) {
	// the image is already current, discard edits
	needToSave |= true;
      } else { // latestNameCheckpointTime == latestEditsCheckpointTime 
	needToSave |= editlog.loadEdits(editsdir.getDirectory()) > 0;
      }

      image.setCheckpointDirectories(null, null);
      
      if (!editlog.isOpen()) {
	editlog.open();
      }
    } catch(IOException e) {
      storage.close();
      throw e;
    }
    return needToSave;
  }
  
  public FSEditLog getLog() {
    return editlog;
  }

  @Override
  public void close() throws IOException {
    image.close();
    editlog.close();
    storage.close();
  }

  /**
     Some methods still require. TODO Fix this
  */
  private StartupOption getStartupOption() {
    return StartupOption.valueOf(conf.get("dfs.namenode.startup",
					  StartupOption.REGULAR.toString()));
  }

  public FSEditLog getEditLog() {
    return editlog;
  }  

  /**
     THIS IS ONLY HERE FOR TESTING!!!!
     Do not use in real code!
  */
  public void setEditLog(FSEditLog editlog) {
    this.editlog = editlog;
  }

  //FIXME
  // used on TestStartup, modified on HDFS-903
  public FSImage getFSImage(){
	return image;
  }
  
  public NNStorage getStorage() {
    return storage;
  }

  public boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
  }
  
  
  public void doFinalize(StorageDirectory sd) throws IOException {
    /*
    File prevDir = sd.getPreviousDir();
    if (!prevDir.exists()) { // already discarded
      LOG.info("Directory " + prevDir + " does not exist.");
      LOG.info("Finalize upgrade for " + sd.getRoot()+ " is not required.");
      return;
    }
    LOG.info("Finalizing upgrade for storage directory " 
             + sd.getRoot() + "."
             + (storage.getLayoutVersion()==0 ? "" :
                   "\n   cur LV = " + this.getLayoutVersion()
                   + "; cur CTime = " + this.getCTime()));
    assert sd.getCurrentDir().exists() : "Current directory must exist.";
    final File tmpDir = sd.getFinalizedTmp();
    // rename previous to tmp and remove
    rename(prevDir, tmpDir);
    deleteDir(tmpDir);
    isUpgradeFinalized = true;
    LOG.info("Finalize upgrade for " + sd.getRoot()+ " is complete.");
    */
  }
  

}
