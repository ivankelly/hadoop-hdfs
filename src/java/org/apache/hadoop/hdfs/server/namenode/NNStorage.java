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
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;

//import org.apache.hadoop.hdfs.server.common.Storage.StorageDirType;


public class NNStorage extends Storage implements Iterable<StorageDirectory> {
  
  public static class LoadDirectory {
    private StorageDirectory directory;
    private boolean needToSave;
    
    public LoadDirectory(StorageDirectory directory, boolean needToSave) {
      this.directory = directory;
      this.needToSave = needToSave;
    }

    public StorageDirectory getDirectory() {
      return directory;
    }

    public boolean getNeedToSave() {
      return needToSave;
    }
  };
  
  protected List<StorageDirectory> removedStorageDirs = new ArrayList<StorageDirectory>();
  
  protected long checkpointTime = -1L;  // The age of the image

  
  public interface StorageErrorListener {
    public void errorOccurred(StorageDirectory sd);
  }

  private Configuration conf;
  private FSNamesystem namesystem;
  private List<StorageErrorListener> errorlisteners;

  private static final Log LOG = LogFactory.getLog(NameNode.class.getName());
  
  static private final FsPermission FILE_PERM = new FsPermission((short)0);
  static private final byte[] PATH_SEPARATOR = DFSUtil.string2Bytes(Path.SEPARATOR);

  private Collection<URI> checkpointDirs;
  private Collection<URI> checkpointEditsDirs;
  
  private volatile int sizeOutputFlushBuffer = 512*1024;

  
  
  private boolean isUpgradeFinalized = false;
  
  // 
  // The filenames used for storing the images
  //
  public enum NameNodeFile {
    IMAGE     ("fsimage"),
    TIME      ("fstime"),
    EDITS     ("edits"),
    IMAGE_NEW ("fsimage.ckpt"),
    EDITS_NEW ("edits.new");
    
    private String fileName = null;
    private NameNodeFile(String name) {this.fileName = name;}
    public String getName() {return fileName;}
  }


  /**
   * Implementation of StorageDirType specific to namenode storage A Storage
   * directory could be of type IMAGE which stores only fsimage, or of type
   * EDITS which stores edits or of type IMAGE_AND_EDITS which stores both
   * fsimage and edits.
   */
  public static enum NameNodeDirType implements StorageDirType {
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
 
  
  ////////////////////////////////////////////////////////////////////////  
  public void registerErrorListener(StorageErrorListener sel) {
    errorlisteners.add(sel);
  }
  
  // TODO +
  public void errorDirectory(StorageDirectory sd) {
    for (StorageErrorListener listener : errorlisteners) {
      listener.errorOccurred(sd);
    }
    
    LOG.info("about to remove corresponding storage:" 
        + sd.getRoot().getAbsolutePath());
    this.removedStorageDirs.add(sd);

  }
  
  
  
  // FIXME
  // to delete. Only temporary change
  protected FSNamesystem getFSNamesystem() {
    return namesystem;
  }
  public void setFSNamesystem(FSNamesystem ns){
    this.namesystem = ns;
  }
  
  private void setDistributedUpgradeState(boolean uState, int uVersion) {
    namesystem.upgradeManager.setUpgradeState(uState, uVersion);
  }
  
  private int getDistributedUpgradeVersion() {
    return namesystem == null ? 0 : namesystem.getDistributedUpgradeVersion();
  }
  
  private boolean getDistributedUpgradeState() {
    return namesystem == null ? false : namesystem.getDistributedUpgradeState();
  }
  
  ///////////////////////////////////////////////////////////////////////
  // PUBLIC API
  ///////////////////////////////////////////////////////////////////////
  
  /** 
   * Constructor
   */
  
  public NNStorage(Configuration conf) throws IOException{
    // FIXME: assert to avoid null values?
    super(NodeType.NAME_NODE);
    this.conf = conf;
    loadStorages(conf);
       
  }

  
  // CHECKPOINT TIME
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
  public void incrementCheckpointTime() {
    setCheckpointTime(checkpointTime + 1);
  }
  
  /** 
   * Set the checkpoint time but don't write it. 
   * FIXME: This probably is not needed.
   */
  public void setCheckpointTimeNoWrite(long newCpt) {
    checkpointTime = newCpt;
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
  public int newNamespaceID() {
    Random r = new Random();
    r.setSeed(now());
    int newID = 0;
    while(newID == 0)
      newID = r.nextInt(0x7FFFFFFF);  // use 31 bits only
    return newID;
  }
  
  void setLayoutVersion(int lv){
    this.layoutVersion = lv;
  }
  
  void setNamespaceId(int nid){
    this.namespaceID = nid;
  }

  public void setCTime(long t){
    this.cTime = t;
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
  
  public File getImageFile(StorageDirectory sd) {
    return getImageFile(sd, NameNodeFile.IMAGE);
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
   * Format a device.
   * @param sd
   * @throws IOException
   */
  public void format(StorageDirectory sd) throws IOException {
    sd.clearDirectory(); // create currrent dir
    sd.lock();
    try {
      //saveCurrent(sd);
      saveFSImage(getImageFile(sd, NameNodeFile.IMAGE));
    } finally {
      sd.unlock();
    }
    LOG.info("Storage directory " + sd.getRoot()
    + " has been successfully formatted.");
  }
  
  
  /**
   * Save the contents of the FS image to the file.
   */
  void saveFSImage(File newFile) throws IOException {
    FSNamesystem fsNamesys = getFSNamesystem();
    FSDirectory fsDir = fsNamesys.dir;
    long startTime = now();
    //
    // Write out data
    //
    FileOutputStream fos = new FileOutputStream(newFile);
    DataOutputStream out = new DataOutputStream(
      new BufferedOutputStream(fos));
    try {
      out.writeInt(FSConstants.LAYOUT_VERSION);
      out.writeInt(getNamespaceID());
      out.writeLong(fsDir.rootDir.numItemsInTree());
      out.writeLong(fsNamesys.getGenerationStamp());
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
      fos.getChannel().force(true);
    } finally {
      out.close();
    }

    LOG.info("Image file of size " + newFile.length() + " saved in " 
        + (now() - startTime)/1000 + " seconds.");
    
  }
  
  
  /**
   * Save file tree image starting from the given root.
   * This is a recursive procedure, which first saves all children of
   * a current directory and then moves inside the sub-directories.
   */
  private static void saveImage(ByteBuffer parentPrefix,
                                int prefixLength,
                                INodeDirectory current,
                                DataOutputStream out) throws IOException {
    int newPrefixLength = prefixLength;
    if (current.getChildrenRaw() == null)
      return;
    for(INode child : current.getChildren()) {
      // print all children first
      parentPrefix.position(prefixLength);
      parentPrefix.put(PATH_SEPARATOR).put(child.getLocalNameBytes());
      saveINode2Image(parentPrefix, child, out);
    }
    for(INode child : current.getChildren()) {
      if(!child.isDirectory())
        continue;
      parentPrefix.position(prefixLength);
      parentPrefix.put(PATH_SEPARATOR).put(child.getLocalNameBytes());
      newPrefixLength = parentPrefix.position();
      saveImage(parentPrefix, newPrefixLength, (INodeDirectory)child, out);
    }
    parentPrefix.position(prefixLength);
  }
  
  
  /*
   * Save one inode's attributes to the image.
   */
  private static void saveINode2Image(ByteBuffer name,
                                      INode node,
                                      DataOutputStream out) throws IOException {
    int nameLen = name.position();
    out.writeShort(nameLen);
    out.write(name.array(), name.arrayOffset(), nameLen);
    if (node.isDirectory()) {
      out.writeShort(0);  // replication
      out.writeLong(node.getModificationTime());
      out.writeLong(0);   // access time
      out.writeLong(0);   // preferred block size
      out.writeInt(-1);   // # of blocks
      out.writeLong(node.getNsQuota());
      out.writeLong(node.getDsQuota());
      FILE_PERM.fromShort(node.getFsPermissionShort());
      PermissionStatus.write(out, node.getUserName(),
                             node.getGroupName(),
                             FILE_PERM);
    } else if (node.isLink()) {
      out.writeShort(0);  // replication
      out.writeLong(0);   // modification time
      out.writeLong(0);   // access time
      out.writeLong(0);   // preferred block size
      out.writeInt(-2);   // # of blocks
      Text.writeString(out, ((INodeSymlink)node).getLinkValue());
      FILE_PERM.fromShort(node.getFsPermissionShort());
      PermissionStatus.write(out, node.getUserName(),
                             node.getGroupName(),
                             FILE_PERM);      
    } else {
      INodeFile fileINode = (INodeFile)node;
      out.writeShort(fileINode.getReplication());
      out.writeLong(fileINode.getModificationTime());
      out.writeLong(fileINode.getAccessTime());
      out.writeLong(fileINode.getPreferredBlockSize());
      Block[] blocks = fileINode.getBlocks();
      out.writeInt(blocks.length);
      for (Block blk : blocks)
        blk.write(out);
      FILE_PERM.fromShort(fileINode.getFsPermissionShort());
      PermissionStatus.write(out, fileINode.getUserName(),
                             fileINode.getGroupName(),
                             FILE_PERM);
    }
  }
  
  
  /**
   * Save current image and empty journal into {@code current} directory.
   */
  public void saveCurrent(StorageDirectory sd) throws IOException {
   
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
 
  /*
   synchronized void createEditLogFile(File name) throws IOException {
   
    //waitForSyncToFinish();

    EditLogOutputStream eStream = new EditLogFileOutputStream(name,
        sizeOutputFlushBuffer);
    eStream.create();
    eStream.close();
  }*/
  

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
  public void moveLastCheckpoint(StorageDirectory sd)
  throws IOException {
    /* FIXME
    File tmpCkptDir = sd.getLastCheckpointTmp();
    File prevCkptDir = sd.getPreviousCheckpoint();
    // remove previous.checkpoint
    if (prevCkptDir.exists())
      deleteDir(prevCkptDir);
    // mv lastcheckpoint.tmp -> previous.checkpoint
    if(tmpCkptDir.exists())
    rename(tmpCkptDir, prevCkptDir);*/
  }

  
  public File getImageFile(StorageDirectory sd, NameNodeFile type) {
    return new File(sd.getCurrentDir(), type.getName());
    //return null;
  }
  
  
  public void clearStorageDirectories() throws IOException {
    this.storageDirs = new ArrayList<StorageDirectory>();
    this.removedStorageDirs = new ArrayList<StorageDirectory>();
  }

  public void addStorageDirectory(URI dir, NameNodeDirType type) throws IOException {
    NNUtils.checkSchemeConsistency(dir);
    
    //  Add to the list of storage directories, only if the 
    // URI is of type file://
    if(dir.getScheme().compareTo(JournalType.FILE.name().toLowerCase()) == 0) {
      storageDirs.add(new StorageDirectory(new File(dir.getPath()), type));
    }
  
  }

  /**
   * This method remove all previous storages.
   * Add the new ones passed as parameters
   * @param fsNameDirs Colletion of URIs where to store fsimage 
   * @param fsEditsDirs Collection of URIs where to store edit logs
   * @throws IOException
   */
  public void setStorageDirectories(Collection<URI> fsNameDirs,
				    Collection<URI> fsEditsDirs) throws IOException {

    this.storageDirs = new ArrayList<StorageDirectory>();
    this.removedStorageDirs = new ArrayList<StorageDirectory>();

    // Add all name dirs with appropriate NameNodeDirType 
    for (URI dirName : fsNameDirs) {
      boolean isAlsoEdits = false;
      for (URI editsDirName : fsEditsDirs) {
        if (editsDirName.compareTo(dirName) == 0) {
          isAlsoEdits = true;
          fsEditsDirs.remove(editsDirName);
          break;
        }
      }
      addStorageDirectory(dirName, (isAlsoEdits) ? NameNodeDirType.IMAGE_AND_EDITS : NameNodeDirType.IMAGE);
    }
    
    // Add edits dirs if they are different from name dirs
    for (URI dirName : fsEditsDirs) {
      addStorageDirectory(dirName, NameNodeDirType.EDITS);
    }
  }
  

  /**
   * In esence, it does the same as 
   * FSNamesystem.getStorageDirs + FSImage.setStorageDirs
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
      
    } else if (dirNames.isEmpty()) {
      dirNames.add("file:///tmp/hadoop/dfs/name");
    }
    
    
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
   * flag that controls if we try to restore failed storages
   */
  private boolean restoreFailedStorage = false;
  
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
  // TODO
  synchronized void attemptRestoreRemovedStorage() {
    
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
          /*if (sd.getStorageDirType().isOfType(NameNodeDirType.EDITS)) {
            File eFile = getEditFile(sd);
            editLog.addNewEditLogStream(eFile);
          }*/
          this.addStorageDir(sd); // restore
          it.remove();
          }
        } catch (IOException e) {
          LOG.warn("failed to restore " + sd.getRoot().getAbsolutePath(),
              e);
        }
    }

  }
  
  @Override
  protected void corruptPreUpgradeStorage(File rootDir) throws IOException {
    
    File oldImageDir = new File(rootDir, "image");
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
      }
  }
  
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
  
  
  public void setCheckpointDirectories(Collection<URI> dirs,
				       Collection<URI> editsDirs) {
    checkpointDirs = dirs;
    checkpointEditsDirs = editsDirs;
  }
  
  /**
   * Analyze storage directories.
   * Recover from previous transitions if required. 
   * Read storage info. 
   * 
   * @param startOpt startup option
   * @throws IOException
   * @return true if the image needs to be saved or false otherwise
   */
  public void initializeDirectories(StartupOption startOpt) throws IOException {
    
    assert startOpt != StartupOption.FORMAT : 
      "NameNode formatting should be performed before reading the image";
    
    // none of the data dirs exist
    if((getNumStorageDirs(NameNodeDirType.IMAGE) == 0 || getNumStorageDirs(NameNodeDirType.EDITS) == 0) 
       && startOpt != StartupOption.IMPORT) {
      throw new IOException("All specified directories are not accessible or do not exist.");
    }

    if(startOpt == StartupOption.IMPORT 
       && (checkpointDirs == null || checkpointDirs.isEmpty())) {
      throw new IOException("Cannot import image from a checkpoint. "
                          + "\"dfs.namenode.checkpoint.dir\" is not set." );
    }

    if(startOpt == StartupOption.IMPORT 
       && (checkpointEditsDirs == null || checkpointEditsDirs.isEmpty())) {
      throw new IOException("Cannot import image from a checkpoint. "
			    + "\"dfs.namenode.checkpoint.dir\" is not set." );
    }
    
    // 1. For each data directory calculate its state and 
    // check whether all is consistent before transitioning.
    Map<StorageDirectory, StorageState> dataDirStates = 
             new HashMap<StorageDirectory, StorageState>();
    boolean isFormatted = false;
    for (StorageDirectory sd : this) {
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
    verifyDistributedUpgradeProgress(startOpt);

    // 2. Format unformatted dirs.
    this.checkpointTime = 0L;
    for (Iterator<StorageDirectory> it = 
                     dirIterator(); it.hasNext();) {
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
  }

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
  
  public boolean recoverInterruptedCheckpoint(StorageDirectory nameSD,
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
  }

  public short adjustReplication(short replication) {
    FSNamesystem fsNamesys = getFSNamesystem();
    short minReplication = fsNamesys.getMinReplication();
    if (replication<minReplication) {
      replication = minReplication;
    }
    short maxReplication = fsNamesys.getMaxReplication();
    if (replication>maxReplication) {
      replication = maxReplication;
    }
    return replication;
  }

  protected void getFields(Properties props, 
                           StorageDirectory sd 
                           ) throws IOException {
    
    //FIXME how to deal with distributed upgrade.
    // For the moment it is using local FSNamesystem. On next part we will handle that.

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
     
    //FIXME how to deal with distributed upgrade
    // For the moment it is using local FSNamesystem. On next part we will handle that.
    
    super.setFields(props, sd);
    boolean uState = getDistributedUpgradeState();
    int uVersion = getDistributedUpgradeVersion();
    if(uState && uVersion != getLayoutVersion()) {
      props.setProperty("distributedUpgradeState", Boolean.toString(uState));
      props.setProperty("distributedUpgradeVersion", Integer.toString(uVersion)); 
    }
    writeCheckpointTime(sd);

  }
  
  /*
   * 
   */
  boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
  }
  
  /*
   * 
   */
  public void doUpgrade() throws IOException {
    
    
    // Do upgrade for each directory
    long oldCTime = this.getCTime();
    this.cTime = now();  // generate new cTime for the state
    int oldLV = this.getLayoutVersion();
    this.layoutVersion = FSConstants.LAYOUT_VERSION;
    this.checkpointTime = now();
    for (Iterator<StorageDirectory> it = 
                           dirIterator(); it.hasNext();) {
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
      //saveCurrent(sd);
      saveFSImage(getImageFile(sd, NameNodeFile.IMAGE));
      
      // rename tmp to previous
      rename(tmpDir, prevDir);
      isUpgradeFinalized = false;
      LOG.info("Upgrade of " + sd.getRoot() + " is complete.");
    }
    //initializeDistributedUpgrade();
    //editLog.open();
  }
  
  public void doFinalize(StorageDirectory sd) throws IOException {
    
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
 
  }
  

  public void doRollback() throws IOException {
    //FIXME
    
    //Move somewhere else
    // Rollback is allowed only if there is 
    // a previous fs states in at least one of the storage directories.
    // Directories that don't have previous state do not rollback
    
    boolean canRollback = false;
    Configuration conf = new HdfsConfiguration();
    NNStorage storage = new NNStorage(conf);
    FSImage prevState = new FSImage(conf,storage);
    prevState.storage.layoutVersion = FSConstants.LAYOUT_VERSION;
    for (Iterator<StorageDirectory> it = 
                       dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists()) {  // use current directory then
        LOG.info("Storage directory " + sd.getRoot()
                 + " does not contain previous fs state.");
        sd.read(); // read and verify consistency with other directories
        continue;
      }
      StorageDirectory sdPrev = prevState.storage.new StorageDirectory(sd.getRoot());
      sdPrev.read(sdPrev.getPreviousVersionFile());  // read and verify consistency of the prev dir
      canRollback = true;
    }
    if (!canRollback)
      throw new IOException("Cannot rollback. " 
                            + "None of the storage directories contain previous fs state.");

    // Now that we know all directories are going to be consistent
    // Do rollback for each directory containing previous state
    for (Iterator<StorageDirectory> it = 
                          dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      File prevDir = sd.getPreviousDir();
      if (!prevDir.exists())
        continue;

      LOG.info("Rolling back storage directory " + sd.getRoot()
               + ".\n   new LV = " + prevState.storage.getLayoutVersion()
               + "; new CTime = " + prevState.storage.getCTime());
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
    verifyDistributedUpgradeProgress(StartupOption.REGULAR);
    
  }
  
  public List<StorageDirectory> getRemovedStorageDirs() {
    return removedStorageDirs;
  }
  
}
