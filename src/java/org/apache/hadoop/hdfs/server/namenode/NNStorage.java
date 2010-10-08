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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.conf.Configuration;

//import org.apache.hadoop.hdfs.server.common.Storage.StorageDirType;


public class NNStorage extends Storage implements Iterable<StorageDirectory> {
  protected long checkpointTime = -1L;  // The age of the image

  public interface StorageErrorListener {
    public void errorOccurred(StorageDirectory sd);
  }

  private Configuration conf;
  private List<StorageErrorListener> errorlisteners;

  private static final Log LOG = LogFactory.getLog(NameNode.class.getName());
  
  
  /**
   * Implementation of StorageDirType specific to namenode storage A Storage
   * directory could be of type IMAGE which stores only fsimage, or of type
   * EDITS which stores edits or of type IMAGE_AND_EDITS which stores both
   * fsimage and edits.
   */
  static enum NameNodeDirType implements StorageDirType {
    UNDEFINED, IMAGE, EDITS, IMAGE_AND_EDITS;

    public StorageDirType getStorageDirType() {
      return this;
    }

    public boolean isOfType(StorageDirType type) {
      if ((this == IMAGE_AND_EDITS) && (type == IMAGE || type == EDITS))
        return true;
      return this == type;
    }
  }
  
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
  
  
  ////////////////////////////////////////////////////////////////////////  
  public void registerErrorListener(StorageErrorListener sel) {
    errorlisteners.add(sel);
  }
  
  // TODO +
  public void errorDirectory(StorageDirectory sd) {
    for (StorageErrorListener listener : errorlisteners) {
      listener.errorOccurred(sd);
    }
  }
  
  
    
  ///////////////////////////////////////////////////////////////////////
  // PUBLIC API
  ///////////////////////////////////////////////////////////////////////
  
  /** 
   * Constructor
   */
  
  public NNStorage(Configuration conf) throws IOException{
    super(NodeType.NAME_NODE);
    this.conf = conf;
    loadStorages(conf);
       
  }

  /**
   * Format a device.
   * @param sd
   * @throws IOException
   */
  public void format(StorageDirectory sd) throws IOException {
    sd.clearDirectory(); // create currrent dir
    sd.lock();
    try {
      saveCurrent(sd);
    } finally {
      sd.unlock();
    }
    LOG.info("Storage directory " + sd.getRoot()
    + " has been successfully formatted.");
  }
  
  // CHECKPOINTING 
  /**
   * The age of the namespace state.<p>
   * Reflects the latest time the image was saved.
   * Modified with every save or a checkpoint.
   * Persisted in VERSION file.
   */
  public long getCheckpointTime() {
    return checkpointTime;
  }  
  
  /**
   * Record new checkpoint time in order to
   * distinguish healthy directories from the removed ones.
   * If there is an error writing new checkpoint time, the corresponding
   * storage directory is removed from the list.
   */
  void incrementCheckpointTime() {
    setCheckpointTime(checkpointTime + 1);
  }
  
  /**
   * The age of the namespace state.<p>
   * Reflects the latest time the image was saved.
   * Modified with every save or a checkpoint.
   * Persisted in VERSION file.
   */
  public void setCheckpointTime(long newCpT) {
    checkpointTime = newCpT;
    // Write new checkpoint time in all storage directories
    for(Iterator<StorageDirectory> it =
    dirIterator(); it.hasNext();) {
    StorageDirectory sd = it.next();
    try {
    writeCheckpointTime(sd);
    } catch(IOException e) {
    // Close any edits stream associated with this dir and remove directory
    LOG.warn("incrementCheckpointTime failed on " + sd.getRoot().getPath() + ";type="+sd.getStorageDirType());
    }
    }
  }
  
  /**
   * Write last checkpoint time into a separate file.
   * 
   * @param sd
   * @throws IOException
   */
  public void writeCheckpointTime(StorageDirectory sd) throws IOException {
    if (checkpointTime < 0L)
      return; // do not write negative time
    File timeFile = getImageFile(sd, NameNodeFile.TIME);
    if (timeFile.exists() && ! timeFile.delete()) {
        LOG.error("Cannot delete chekpoint time file: "
                  + timeFile.getCanonicalPath());
    }
    FileOutputStream fos = new FileOutputStream(timeFile);
    DataOutputStream out = new DataOutputStream(fos);
    try {
      out.writeLong(checkpointTime);
      out.flush();
      fos.getChannel().force(true);
    } finally {
      out.close();
    }
  }
  
  
  

  
  synchronized public long getEditsTime() {
    Iterator<StorageDirectory> it = dirIterator(NameNodeDirType.EDITS);
    if(it.hasNext())
      return getEditFile(it.next()).lastModified();
    return 0;
  }

  
  /**
   * Return number of storage directories of the given type.
   * @param dirType directory type
   * @return number of storage directories of type dirType
   */
  public int getNumStorageDirs(NameNodeDirType dirType) {
    if(dirType == null)
      return getNumStorageDirs();
    Iterator<StorageDirectory> it = dirIterator(dirType);
    int numDirs = 0;
    for(; it.hasNext(); it.next())
      numDirs++;
    return numDirs;
  }
  
  /**
   * Retrieve current directories of type EDITS
   * 
   * @return Collection of URI representing edits directories
   * @throws IOException
   *             in case of URI processing error
   */
  public Collection<URI> getEditsDirectories() throws IOException {
    return getDirectories(NameNodeDirType.EDITS);
    //return null;
  }
  
  /**
   * Retrieve current directories of type IMAGE
   * 
   * @return Collection of URI representing image directories
   * @throws IOException
   *             in case of URI processing error
   */
  
  public Collection<URI> getImageDirectories() throws IOException {
    return getDirectories(NameNodeDirType.IMAGE);
    //return null;
  }
  
  
  public File getEditFile(StorageDirectory sd) {
    return getImageFile(sd, NameNodeFile.EDITS);
    //return null;
  }
  
  public File getEditNewFile(StorageDirectory sd) {
    return getImageFile(sd, NameNodeFile.EDITS_NEW);
    //return null;
  }

  public String listStorageDirectories() {
    StringBuilder buf = new StringBuilder();
    for (StorageDirectory sd : storageDirs) {
      buf.append(sd.getRoot() + "(" + sd.getStorageDirType() + ");");
    }
    return buf.toString();
  }
  
  
  public Iterator<StorageDirectory> iterator() {
    return dirIterator();
  }
  
  
  Collection<URI> getDirectories(NameNodeDirType dirType) 
  throws IOException {
    ArrayList<URI> list = new ArrayList<URI>();
    Iterator<StorageDirectory> it = (dirType == null) ? dirIterator() :
                                dirIterator(dirType);
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
  
  
  @Override
  public boolean isConversionNeeded(StorageDirectory sd) throws IOException {
      /*File oldImageDir = new File(sd.getRoot(), "image");
    if (!oldImageDir.exists()) {
      if(sd.getVersionFile().exists())
        throw new InconsistentFSStateException(sd.getRoot(),
            oldImageDir + " does not exist.");
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
      }*/
    return true;
  }
  
  
  /**
   * Save current image and empty journal into {@code current} directory.
   */
  public void saveCurrent(StorageDirectory sd) throws IOException {
/*    File curDir = sd.getCurrentDir();
    NameNodeDirType dirType = (NameNodeDirType)sd.getStorageDirType();
    // save new image or new edits
    if (!curDir.exists() && !curDir.mkdir())
      throw new IOException("Cannot create directory " + curDir);
    if (dirType.isOfType(NameNodeDirType.IMAGE))
      saveFSImage(getImageFile(sd, NameNodeFile.IMAGE));
    if (dirType.isOfType(NameNodeDirType.EDITS))
      editLog.createEditLogFile(getImageFile(sd, NameNodeFile.EDITS));
    // write version and time files
    sd.write();
    */
  }


  /**
   * Move {@code current} to {@code lastcheckpoint.tmp} and
   * recreate empty {@code current}.
   * {@code current} is moved only if it is well formatted,
   * that is contains VERSION file.
   * 
   * @see org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory#getLastCheckpointTmp()
   * @see org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory#getPreviousCheckpoint()
   */
  public void moveCurrent(StorageDirectory sd)
  throws IOException {
    /*    File curDir = sd.getCurrentDir();
    File tmpCkptDir = sd.getLastCheckpointTmp();
    // mv current -> lastcheckpoint.tmp
    // only if current is formatted - has VERSION file
    if(sd.getVersionFile().exists()) {
      assert curDir.exists() : curDir + " directory must exist.";
      assert !tmpCkptDir.exists() : tmpCkptDir + " directory must not exist.";
      rename(curDir, tmpCkptDir);
    }
    // recreate current
    if(!curDir.exists() && !curDir.mkdir())
    throw new IOException("Cannot create directory " + curDir);*/
  }

  /**
   * Move {@code lastcheckpoint.tmp} to {@code previous.checkpoint}
   * 
   * @see org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory#getPreviousCheckpoint()
   * @see org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory#getLastCheckpointTmp()
   */
  public void moveLastCheckpoint(StorageDirectory sd)
  throws IOException {
    /*
    File tmpCkptDir = sd.getLastCheckpointTmp();
    File prevCkptDir = sd.getPreviousCheckpoint();
    // remove previous.checkpoint
    if (prevCkptDir.exists())
      deleteDir(prevCkptDir);
    // mv lastcheckpoint.tmp -> previous.checkpoint
    if(tmpCkptDir.exists())
    rename(tmpCkptDir, prevCkptDir);*/
  }

  
  

  ///////////////////////////////////////////////////////////////////////
  // PRIVATE methods
  ///////////////////////////////////////////////////////////////////////
    
  static protected File getImageFile(StorageDirectory sd, NameNodeFile type) {
    return new File(sd.getCurrentDir(), type.getName());
    //return null;
  }

  /**
   * In esence, it does the same as 
   * FSNamesystem.getStorageDirs + FSImage.setstoragedirs
   */
  private void loadStorages(Configuration conf) throws IOException{
    
    Collection<String> dirNames = conf.getStringCollection(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
    Collection<String> editsNames = conf.getStringCollection(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY);
  
    StartupOption startOpt = NameNode.getStartupOption(conf);
    if(startOpt == StartupOption.IMPORT) {
      // In case of IMPORT this will get rid of default directories 
      // but will retain directories specified in hdfs-site.xml
      // When importing image from a checkpoint, the name-node can
      // start with empty set of storage directories.
      Configuration cE = new HdfsConfiguration(false);
      cE.addResource("core-default.xml");
      cE.addResource("core-site.xml");
      cE.addResource("hdfs-default.xml");

      dirNames.removeAll(
          cE.getStringCollection(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY));
      editsNames.removeAll(
          cE.getStringCollection(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY));
      
      if(dirNames.isEmpty() || editsNames.isEmpty() ){
        String property = dirNames.isEmpty() ? DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY.toString() : "";
        property += dirNames.isEmpty() ? " and " + DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY.toString() : "";
        
        LOG.warn("!!! WARNING !!!" +
            "\n\tThe NameNode currently runs without persistent storage." +
            "\n\tAny changes to the file system meta-data may be lost." +
            "\n\tRecommended actions:" +
            "\n\t\t- shutdown and restart NameNode with configured \""
            + property + "\" in hdfs-site.xml;" +
            "\n\t\t- use Backup Node as a persistent and up-to-date storage " +
        "of the file system meta-data.");
      }
      
      
    } else if (dirNames.isEmpty())
      dirNames.add("file:///tmp/hadoop/dfs/name");
    
    
    Collection<URI> fsNameDirs = Util.stringCollectionAsURIs(dirNames);
    Collection<URI> fsEditsDirs = Util.stringCollectionAsURIs(editsNames);
        
    this.storageDirs = new ArrayList<StorageDirectory>();
    //this.removedStorageDirs = new ArrayList<StorageDirectory>();
      
          
      // Add all name dirs with appropriate NameNodeDirType 
      for (URI dirName : fsNameDirs) {
        NNUtils.checkSchemeConsistency(dirName);
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
        NNUtils.checkSchemeConsistency(dirName);
        // Add to the list of storage directories, only if the 
        // URI is of type file://
        if(dirName.getScheme().compareTo(JournalType.FILE.name().toLowerCase())
            == 0)
          this.addStorageDir(new StorageDirectory(new File(dirName.getPath()), 
                      NameNodeDirType.EDITS));
      }
    
  }

  /**
   * See if any of removed storages iw "writable" again, and can be returned 
   * into service
   */
  // TODO
  synchronized void attemptRestoreRemovedStorage() {
      /*
    // if directory is "alive" - copy the images there...
    if (!restoreFailedStorage || removedStorageDirs.size() == 0)
      return; // nothing to restore

    LOG.info("FSImage.attemptRestoreRemovedStorage: check removed(failed) "
        + "storarge. removedStorages size = "
        + removedStorageDirs.size());
    for (Iterator<StorageDirectory> it = this.removedStorageDirs.iterator(); 
          it.hasNext();) {
      StorageDirectory sd = it.next();
      File root = sd.getRoot();
      LOG.info("currently disabled dir " + root.getAbsolutePath()
          + "; type=" + sd.getStorageDirType() + ";canwrite="
                   + root.canWrite());
      try {
        if (root.exists() && root.canWrite()) {
          format(sd);
          LOG.info("restoring dir " + sd.getRoot().getAbsolutePath());
          if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
            File eFile = getEditFile(sd);
            editLog.addNewEditLogStream(eFile);
          }
          this.addStorageDir(sd); // restore
          it.remove();
          }
        } catch (IOException e) {
          LOG.warn("failed to restore " + sd.getRoot().getAbsolutePath(),
              e);
        }
        }*/
  }
  
  @Override
  protected void corruptPreUpgradeStorage(File rootDir) throws IOException {
    /*File oldImageDir = new File(rootDir, "image");
    if (!oldImageDir.exists())
      if (!oldImageDir.mkdir())
        throw new IOException("Cannot create directory " + oldImageDir);
    File oldImage = new File(oldImageDir, "fsimage");
    if (!oldImage.exists())
      // recreate old image file to let pre-upgrade versions fail
      if (!oldImage.createNewFile())
        throw new IOException("Cannot create file " + oldImage);
    RandomAccessFile oldFile = new RandomAccessFile(oldImage, "rws");
    // write new version into old image file
    try {
      writeCorruptedData(oldFile);
    } finally {
      oldFile.close();
      }*/
  }
  


  
}
