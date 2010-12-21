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

import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.Closeable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Properties;
import java.io.RandomAccessFile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.UpgradeManager;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;

import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import org.apache.hadoop.io.MD5Hash;


public class NNStorage extends Storage implements Iterable<StorageDirectory>, Closeable {
  private static final Log LOG = LogFactory.getLog(NNStorage.class.getName());

  public static final String MESSAGE_DIGEST_PROPERTY = "imageMD5Digest";

  //
  // The filenames used for storing the images
  //
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

  /**
   * Implementation of StorageDirType specific to namenode storage
   * A Storage directory could be of type IMAGE which stores only fsimage,
   * or of type EDITS which stores edits or of type IMAGE_AND_EDITS which 
   * stores both fsimage and edits.
   */
  static enum NameNodeDirType implements StorageDirType {
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
  }

  public interface StorageListener {
    public void errorOccurred(StorageDirectory sd) throws IOException;
    public void formatOccurred(StorageDirectory sd) throws IOException;
    public void directoryAvailable(StorageDirectory sd) throws IOException;
  }

  private Configuration conf;
  private List<StorageListener> listeners;
  private UpgradeManager upgradeManager = null;
  protected MD5Hash imageDigest = null;

  /**
   * flag that controls if we try to restore failed storages
   */
  private boolean restoreFailedStorage = false;
  private boolean disablePreUpgradableLayoutCheck = false;

  public long checkpointTime = -1L;  // The age of the image

  /**
   * list of failed (and thus removed) storages
   */
  protected List<StorageDirectory> removedStorageDirs = new ArrayList<StorageDirectory>();

  public NNStorage(Configuration conf) {
    super(NodeType.NAME_NODE);
    this.conf = conf;
    this.listeners = new ArrayList<StorageListener>();
  }

  @Override
  public boolean isPreUpgradableLayout(StorageDirectory sd) throws IOException {
    if (disablePreUpgradableLayoutCheck) {
      return false;
    }

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
  }

  /** 
   * Methods to all storage to be iterated over
   */
  @Override
  public Iterator<StorageDirectory> iterator() {
    return dirIterator();
  }
  
  public Iterable<StorageDirectory> iterable(final NameNodeDirType type) {
    return new Iterable<StorageDirectory>() {
        public Iterator<StorageDirectory> iterator() {
	  return dirIterator(type);
	}
    };
  }
  
  @Override
  synchronized public void close() throws IOException {
    unlockAll();
  }

  public void setRestoreFailedStorage(boolean val) {
    LOG.info("set restore failed storage to " + val);
    restoreFailedStorage=val;
  }
  
  public boolean getRestoreFailedStorage() {
    return restoreFailedStorage;
  }
  
  /**
   * See if any of removed storages iw "writable" again, and can be returned 
   * into service
   */
  synchronized public void attemptRestoreRemovedStorage() {   
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
          
          for (StorageListener listener : listeners) {
            listener.directoryAvailable(sd);
          }

          this.addStorageDir(sd); // restore
          it.remove();
        }
      } catch(IOException e) {
        LOG.warn("failed to restore " + sd.getRoot().getAbsolutePath(), e);
      }
    }    
  }
  
  public List<StorageDirectory> getRemovedStorageDirs() {
    return this.removedStorageDirs;
  }

  public void setStorageDirectories(Collection<URI> fsNameDirs,
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
  }

  /* 
   * Checks the consistency of a URI, in particular if the scheme 
   * is specified and is supported by a concrete implementation 
   */
  public static void checkSchemeConsistency(URI u) throws IOException {
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
  }

  /**
   * Retrieve current directories of type IMAGE
   * @return Collection of URI representing image directories 
   * @throws IOException in case of URI processing error
   */
  public Collection<URI> getImageDirectories() throws IOException {
    return getDirectories(NameNodeDirType.IMAGE);
  }

  /**
   * Retrieve current directories of type EDITS
   * @return Collection of URI representing edits directories 
   * @throws IOException in case of URI processing error
   */
  public Collection<URI> getEditsDirectories() throws IOException {
    return getDirectories(NameNodeDirType.EDITS);
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
  
  public Collection<URI> getDirectories(NameNodeDirType dirType) 
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

  /**
   * Determine the checkpoint time of the specified StorageDirectory
   * 
   * @param sd StorageDirectory to check
   * @return If file exists and can be read, last checkpoint time. If not, 0L.
   * @throws IOException On errors processing file pointed to by sd
   */
  public long readCheckpointTime(StorageDirectory sd) throws IOException {
    File timeFile = getImageFile(sd, NameNodeFile.TIME);
    long timeStamp = 0L;
    if (timeFile.exists() && timeFile.canRead()) {
      DataInputStream in = new DataInputStream(new FileInputStream(timeFile));
      try {
        timeStamp = in.readLong();
      } finally {
        in.close();
      }
    }
    return timeStamp;
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

  /**
   * Record new checkpoint time in order to
   * distinguish healthy directories from the removed ones.
   * If there is an error writing new checkpoint time, the corresponding
   * storage directory is removed from the list.
   */
  public void incrementCheckpointTime() {
    setCheckpointTime(checkpointTime + 1);
  }

  /**
   * The age of the namespace state.<p>
   * Reflects the latest time the image was saved.
   * Modified with every save or a checkpoint.
   * Persisted in VERSION file.
   */
  public long getCheckpointTime() {
    return checkpointTime;
  }

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
   * Return the name of the image file that is uploaded by periodic
   * checkpointing.
   */
  public File[] getFsImageNameCheckpoint() {
    ArrayList<File> list = new ArrayList<File>();
    for (Iterator<StorageDirectory> it = 
                 dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      list.add(getImageFile(it.next(), NameNodeFile.IMAGE_NEW));
    }
    return list.toArray(new File[list.size()]);
  }

    /**
   * Return the name of the image file.
   */
  public File getFsImageName() {
    StorageDirectory sd = null;
    for (Iterator<StorageDirectory> it = 
      dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
      sd = it.next();
      if(sd.getRoot().canRead())
        return getImageFile(sd, NameNodeFile.IMAGE); 
    }
    return null;
  }

  
  public File getFsEditName() throws IOException {
    for (StorageDirectory sd : iterable(NameNodeDirType.EDITS)) {
      if(sd.getRoot().canRead())
        return getEditFile(sd);
    }
    return null;
  }

  public File getFsTimeName() {
    StorageDirectory sd = null;
    // NameNodeFile.TIME shoul be same on all directories
    for (Iterator<StorageDirectory> it = 
             dirIterator(); it.hasNext();)
      sd = it.next();
    return getImageFile(sd, NameNodeFile.TIME);
  }

  /** Create new dfs name directory.  Caution: this destroys all files
   * in this filesystem. */
  void format(StorageDirectory sd) throws IOException {
    sd.clearDirectory(); // create currrent dir
        
    for (StorageListener listener : listeners) {
      listener.formatOccurred(sd);
    }
    sd.write();

    LOG.info("Storage directory " + sd.getRoot()
             + " has been successfully formatted.");
  }

  public void format() throws IOException {
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    this.namespaceID = newNamespaceID();
    this.cTime = 0L;
    this.checkpointTime = now();
    for (Iterator<StorageDirectory> it = 
                           dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      format(sd);
    }
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
  private int newNamespaceID() {
    Random r = new Random();
    r.setSeed(now());
    int newID = 0;
    while(newID == 0)
      newID = r.nextInt(0x7FFFFFFF);  // use 31 bits only
    return newID;
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
  protected void moveCurrent(StorageDirectory sd)
    throws IOException {
    File curDir = sd.getCurrentDir();
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
      throw new IOException("Cannot create directory " + curDir);
  }

  /**
   * Move {@code lastcheckpoint.tmp} to {@code previous.checkpoint}
   * 
   * @see org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory#getPreviousCheckpoint()
   * @see org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory#getLastCheckpointTmp()
   */
  protected void moveLastCheckpoint(StorageDirectory sd)
    throws IOException {
    File tmpCkptDir = sd.getLastCheckpointTmp();
    File prevCkptDir = sd.getPreviousCheckpoint();
    // remove previous.checkpoint
    if (prevCkptDir.exists())
      deleteDir(prevCkptDir);
    // mv lastcheckpoint.tmp -> previous.checkpoint
    if(tmpCkptDir.exists())
      rename(tmpCkptDir, prevCkptDir);
  }

  protected void getFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    super.getFields(props, sd);
    if (layoutVersion == 0)
      throw new IOException("NameNode directory " 
                            + sd.getRoot() + " is not formatted.");
    String sDUS, sDUV;
    sDUS = props.getProperty("distributedUpgradeState"); 
    sDUV = props.getProperty("distributedUpgradeVersion");
    setDistributedUpgradeState(
        sDUS == null? false : Boolean.parseBoolean(sDUS),
        sDUV == null? getLayoutVersion() : Integer.parseInt(sDUV));
    
    String sMd5 = props.getProperty(MESSAGE_DIGEST_PROPERTY);
    if (layoutVersion <= -26) {
      if (sMd5 == null) {
        throw new InconsistentFSStateException(sd.getRoot(),
            "file " + STORAGE_FILE_VERSION + " does not have MD5 image digest.");
      }
      this.imageDigest = new MD5Hash(sMd5);
    } else if (sMd5 != null) {
      throw new InconsistentFSStateException(sd.getRoot(),
          "file " + STORAGE_FILE_VERSION +
          " has image MD5 digest when version is " + layoutVersion);
    }

    this.checkpointTime = readCheckpointTime(sd);
  }

  /**
   * Write last checkpoint time and version file into the storage directory.
   * 
   * The version file should always be written last.
   * Missing or corrupted version file indicates that 
   * the checkpoint is not valid.
   * 
   * @param sd storage directory
   * @throws IOException
   */
  protected void setFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    super.setFields(props, sd);
    boolean uState = getDistributedUpgradeState();
    int uVersion = getDistributedUpgradeVersion();
    if(uState && uVersion != getLayoutVersion()) {
      props.setProperty("distributedUpgradeState", Boolean.toString(uState));
      props.setProperty("distributedUpgradeVersion", Integer.toString(uVersion)); 
    }
    if (imageDigest == null) {
      imageDigest = MD5Hash.digest(
          new FileInputStream(getImageFile(sd, NameNodeFile.IMAGE)));
    }
        
    props.setProperty(MESSAGE_DIGEST_PROPERTY, imageDigest.toString());

    writeCheckpointTime(sd);
  }

  static public File getImageFile(StorageDirectory sd, NameNodeFile type) {
    return new File(sd.getCurrentDir(), type.getName());
  }
    
  public File getEditFile(StorageDirectory sd) {
    return getImageFile(sd, NameNodeFile.EDITS);
  }
  
  public File getEditNewFile(StorageDirectory sd) {
    return getImageFile(sd, NameNodeFile.EDITS_NEW);
  }

  public Collection<File> getFiles(NameNodeFile type, NameNodeDirType dirType) {
    ArrayList<File> list = new ArrayList<File>();
    Iterator<StorageDirectory> it = 
      (dirType == null) ? iterator() : iterable(dirType).iterator();
    for ( ;it.hasNext(); ) {
      list.add(getImageFile(it.next(), type));
    }
    return list;
  }

  public void setUpgradeManager(UpgradeManager um) {
    upgradeManager = um;
  }

  public boolean getDistributedUpgradeState() {
    return upgradeManager == null ? false : upgradeManager.getUpgradeState();
  }

  public  int getDistributedUpgradeVersion() {
    return upgradeManager == null ? 0 : upgradeManager.getUpgradeVersion();
  }

  private void setDistributedUpgradeState(boolean uState, int uVersion) {
    upgradeManager.setUpgradeState(uState, uVersion);
  }

  public void verifyDistributedUpgradeProgress(StartupOption startOpt
                                                ) throws IOException {
    if(startOpt == StartupOption.ROLLBACK || startOpt == StartupOption.IMPORT)
      return;

    assert upgradeManager != null : "FSNameSystem.upgradeManager is null.";
    if(startOpt != StartupOption.UPGRADE) {
      if(upgradeManager.getUpgradeState())
        throw new IOException(
                    "\n   Previous distributed upgrade was not completed. "
                  + "\n   Please restart NameNode with -upgrade option.");
      if(upgradeManager.getDistributedUpgrades() != null)
        throw new IOException("\n   Distributed upgrade for NameNode version " 
                              + upgradeManager.getUpgradeVersion() 
                              + " to current LV " + FSConstants.LAYOUT_VERSION
                              + " is required.\n   Please restart NameNode"
                              + " with -upgrade option.");
    }
  }

  public void initializeDistributedUpgrade() throws IOException {
    if(! upgradeManager.initializeUpgrade())
      return;
    // write new upgrade state into disk
    writeAll();
    LOG.info("\n   Distributed upgrade for NameNode version " 
             + upgradeManager.getUpgradeVersion() + " to current LV " 
             + FSConstants.LAYOUT_VERSION + " is initialized.");
  }
  
  public void setImageDigest(MD5Hash digest) {
    this.imageDigest = digest;
  }
  
  public MD5Hash getImageDigest() {
    return imageDigest;
  }

  public void registerListener(StorageListener sel) {
    listeners.add(sel);
  }

  public void setDisablePreUpgradableLayoutCheck(boolean val) {
    disablePreUpgradableLayoutCheck = val;
  }

  public synchronized void errorDirectories(List<StorageDirectory> sds) throws IOException {
    for (StorageDirectory sd : sds) {
      errorDirectory(sd);
    }
  }

  public synchronized void errorDirectory(StorageDirectory sd) throws IOException {
    String lsd = listStorageDirectories();
    LOG.info("current list of storage dirs:" + lsd);
    
    for (StorageListener listener : listeners) {
      listener.errorOccurred(sd);
    }
    
    LOG.info("about to remove corresponding storage:" 
	     + sd.getRoot().getAbsolutePath());
    try {
      sd.unlock();
    } catch (Exception e) {
      // do nothing
    }

    this.removedStorageDirs.add(sd);
    this.storageDirs.remove(sd);
    
    incrementCheckpointTime();
    
    lsd = listStorageDirectories();
    LOG.info("at the end current list of storage dirs:" + lsd);
  }

  public void clearStorageDirectories() {
    storageDirs = new ArrayList<StorageDirectory>();
  }
}