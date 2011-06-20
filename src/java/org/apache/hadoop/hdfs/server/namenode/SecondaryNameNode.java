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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtil.ErrorSimulator;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.namenode.NNStorageArchivalManager.StorageArchiver;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Krb5AndCertsSslSocketConnector;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;

import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**********************************************************
 * The Secondary NameNode is a helper to the primary NameNode.
 * The Secondary is responsible for supporting periodic checkpoints 
 * of the HDFS metadata. The current design allows only one Secondary
 * NameNode per HDFs cluster.
 *
 * The Secondary NameNode is a daemon that periodically wakes
 * up (determined by the schedule specified in the configuration),
 * triggers a periodic checkpoint and then goes back to sleep.
 * The Secondary NameNode uses the ClientProtocol to talk to the
 * primary NameNode.
 *
 **********************************************************/
@Deprecated // use BackupNode with -checkpoint argument instead.
@InterfaceAudience.Private
public class SecondaryNameNode implements Runnable {
    
  static{
    HdfsConfiguration.init();
  }
  public static final Log LOG = 
    LogFactory.getLog(SecondaryNameNode.class.getName());

  private final long starttime = System.currentTimeMillis();
  private volatile long lastCheckpointTime = 0;

  private String fsName;
  private CheckpointStorage checkpointImage;

  private NamenodeProtocol namenode;
  private Configuration conf;
  private InetSocketAddress nameNodeAddr;
  private volatile boolean shouldRun;
  private HttpServer infoServer;
  private int infoPort;
  private int imagePort;
  private String infoBindAddress;

  private Collection<URI> checkpointDirs;
  private Collection<URI> checkpointEditsDirs;
  private long checkpointPeriod;    // in seconds
  private long checkpointSize;    // size (in bytes) of current Edit Log

  /** {@inheritDoc} */
  public String toString() {
    return getClass().getSimpleName() + " Status" 
      + "\nName Node Address    : " + nameNodeAddr   
      + "\nStart Time           : " + new Date(starttime)
      + "\nLast Checkpoint Time : " + (lastCheckpointTime == 0? "--": new Date(lastCheckpointTime))
      + "\nCheckpoint Period    : " + checkpointPeriod + " seconds"
      + "\nCheckpoint Size      : " + StringUtils.byteDesc(checkpointSize)
                                    + " (= " + checkpointSize + " bytes)" 
      + "\nCheckpoint Dirs      : " + checkpointDirs
      + "\nCheckpoint Edits Dirs: " + checkpointEditsDirs;
  }

  @VisibleForTesting
  FSImage getFSImage() {
    return checkpointImage;
  }
  
  @VisibleForTesting
  void setFSImage(CheckpointStorage image) {
    this.checkpointImage = image;
  }
  
  @VisibleForTesting
  NamenodeProtocol getNameNode() {
    return namenode;
  }
  
  @VisibleForTesting
  void setNameNode(NamenodeProtocol namenode) {
    this.namenode = namenode;
  }

  @VisibleForTesting
  List<URI> getCheckpointDirs() {
    return ImmutableList.copyOf(checkpointDirs);
  }
  
  /**
   * Create a connection to the primary namenode.
   */
  public SecondaryNameNode(Configuration conf)  throws IOException {
    try {
      NameNode.initializeGenericKeys(conf);
      initialize(conf);
    } catch(IOException e) {
      shutdown();
      throw e;
    }
  }
  
  public static InetSocketAddress getHttpAddress(Configuration conf) {
    return NetUtils.createSocketAddr(conf.get(
        DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
        DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_DEFAULT));
  }
  
  /**
   * Initialize SecondaryNameNode.
   */
  private void initialize(final Configuration conf) throws IOException {
    final InetSocketAddress infoSocAddr = getHttpAddress(conf);
    infoBindAddress = infoSocAddr.getHostName();
    UserGroupInformation.setConfiguration(conf);
    if (UserGroupInformation.isSecurityEnabled()) {
      SecurityUtil.login(conf, 
          DFSConfigKeys.DFS_SECONDARY_NAMENODE_KEYTAB_FILE_KEY,
          DFSConfigKeys.DFS_SECONDARY_NAMENODE_USER_NAME_KEY,
          infoBindAddress);
    }
    // initiate Java VM metrics
    JvmMetrics.create("SecondaryNameNode",
        conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY),
        DefaultMetricsSystem.instance());
    
    // Create connection to the namenode.
    shouldRun = true;
    nameNodeAddr = NameNode.getServiceAddress(conf, true);

    this.conf = conf;
    this.namenode =
        (NamenodeProtocol) RPC.waitForProxy(NamenodeProtocol.class,
            NamenodeProtocol.versionID, nameNodeAddr, conf);

    // initialize checkpoint directories
    fsName = getInfoServer();
    checkpointDirs = FSImage.getCheckpointDirs(conf,
                                  "/tmp/hadoop/dfs/namesecondary");
    checkpointEditsDirs = FSImage.getCheckpointEditsDirs(conf, 
                                  "/tmp/hadoop/dfs/namesecondary");    
    checkpointImage = new CheckpointStorage(conf, checkpointDirs, checkpointEditsDirs);
    checkpointImage.recoverCreate();

    // Initialize other scheduling parameters from the configuration
    checkpointPeriod = conf.getLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, 
                                    DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT);
    checkpointSize = conf.getLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_SIZE_KEY, 
                                  DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_SIZE_DEFAULT);

    // initialize the webserver for uploading files.
    // Kerberized SSL servers must be run from the host principal...
    UserGroupInformation httpUGI = 
      UserGroupInformation.loginUserFromKeytabAndReturnUGI(
          SecurityUtil.getServerPrincipal(conf
              .get(DFSConfigKeys.DFS_SECONDARY_NAMENODE_KRB_HTTPS_USER_NAME_KEY),
              infoBindAddress),
          conf.get(DFSConfigKeys.DFS_SECONDARY_NAMENODE_KEYTAB_FILE_KEY));
    try {
      infoServer = httpUGI.doAs(new PrivilegedExceptionAction<HttpServer>() {
        @Override
        public HttpServer run() throws IOException, InterruptedException {
          LOG.info("Starting web server as: " +
              UserGroupInformation.getCurrentUser().getUserName());

          int tmpInfoPort = infoSocAddr.getPort();
          infoServer = new HttpServer("secondary", infoBindAddress, tmpInfoPort,
              tmpInfoPort == 0, conf, 
              new AccessControlList(conf.get(DFSConfigKeys.DFS_ADMIN, " ")));
          
          if(UserGroupInformation.isSecurityEnabled()) {
            System.setProperty("https.cipherSuites", 
                Krb5AndCertsSslSocketConnector.KRB5_CIPHER_SUITES.get(0));
            InetSocketAddress secInfoSocAddr = 
              NetUtils.createSocketAddr(infoBindAddress + ":"+ conf.get(
                "dfs.secondary.https.port", infoBindAddress + ":" + 0));
            imagePort = secInfoSocAddr.getPort();
            infoServer.addSslListener(secInfoSocAddr, conf, false, true);
          }
          
          infoServer.setAttribute("secondary.name.node", this);
          infoServer.setAttribute("name.system.image", checkpointImage);
          infoServer.setAttribute(JspHelper.CURRENT_CONF, conf);
          infoServer.addInternalServlet("getimage", "/getimage",
              GetImageServlet.class, true);
          infoServer.start();
          return infoServer;
        }
      });
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } 
    
    LOG.info("Web server init done");

    // The web-server port can be ephemeral... ensure we have the correct info
    infoPort = infoServer.getPort();
    if(!UserGroupInformation.isSecurityEnabled())
      imagePort = infoPort;
    conf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, infoBindAddress + ":" +infoPort); 
    LOG.info("Secondary Web-server up at: " + infoBindAddress + ":" +infoPort);
    LOG.info("Secondary image servlet up at: " + infoBindAddress + ":" + imagePort);
    LOG.warn("Checkpoint Period   :" + checkpointPeriod + " secs " +
             "(" + checkpointPeriod/60 + " min)");
    LOG.warn("Log Size Trigger    :" + checkpointSize + " bytes " +
             "(" + checkpointSize/1024 + " KB)");
  }

  /**
   * Shut down this instance of the datanode.
   * Returns only after shutdown is complete.
   */
  public void shutdown() {
    shouldRun = false;
    try {
      if (infoServer != null) infoServer.stop();
    } catch (Exception e) {
      LOG.warn("Exception shutting down SecondaryNameNode", e);
    }
    try {
      if (checkpointImage != null) checkpointImage.close();
    } catch(IOException e) {
      LOG.warn(StringUtils.stringifyException(e));
    }
  }

  public void run() {
    if (UserGroupInformation.isSecurityEnabled()) {
      UserGroupInformation ugi = null;
      try { 
        ugi = UserGroupInformation.getLoginUser();
      } catch (IOException e) {
        LOG.error(StringUtils.stringifyException(e));
        e.printStackTrace();
        Runtime.getRuntime().exit(-1);
      }
      ugi.doAs(new PrivilegedAction<Object>() {
        @Override
        public Object run() {
          doWork();
          return null;
        }
      });
    } else {
      doWork();
    }
  }
  //
  // The main work loop
  //
  public void doWork() {

    //
    // Poll the Namenode (once every 5 minutes) to find the size of the
    // pending edit log.
    //
    long period = 5 * 60;              // 5 minutes
    if (checkpointPeriod < period) {
      period = checkpointPeriod;
    }

    while (shouldRun) {
      try {
        Thread.sleep(1000 * period);
      } catch (InterruptedException ie) {
        // do nothing
      }
      if (!shouldRun) {
        break;
      }
      try {
        // We may have lost our ticket since last checkpoint, log in again, just in case
        if(UserGroupInformation.isSecurityEnabled())
          UserGroupInformation.getCurrentUser().reloginFromKeytab();
        
        long now = System.currentTimeMillis();

        long size = namenode.getEditLogSize();
        if (size >= checkpointSize || 
            now >= lastCheckpointTime + 1000 * checkpointPeriod) {
          doCheckpoint();
          lastCheckpointTime = now;
        }
      } catch (IOException e) {
        LOG.error("Exception in doCheckpoint: ");
        LOG.error(StringUtils.stringifyException(e));
        e.printStackTrace();
      } catch (Throwable e) {
        LOG.error("Throwable Exception in doCheckpoint: ");
        LOG.error(StringUtils.stringifyException(e));
        e.printStackTrace();
        Runtime.getRuntime().exit(-1);
      }
    }
  }

  /**
   * Download <code>fsimage</code> and <code>edits</code>
   * files from the name-node.
   * @return true if a new image has been downloaded and needs to be loaded
   * @throws IOException
   */
  static boolean downloadCheckpointFiles(
      final String nnHostPort,
      final FSImage dstImage,
      final CheckpointSignature sig,
      final RemoteEditLogManifest manifest
  ) throws IOException {
    try {
        Boolean b = UserGroupInformation.getCurrentUser().doAs(
            new PrivilegedExceptionAction<Boolean>() {
  
          @Override
          public Boolean run() throws Exception {
            dstImage.getStorage().cTime = sig.cTime;

            // get fsimage
            boolean downloadImage = true;
            if (sig.mostRecentCheckpointTxId ==
                dstImage.getStorage().getMostRecentCheckpointTxId()) {
              downloadImage = false;
              LOG.info("Image has not changed. Will not download image.");
            } else {
              MD5Hash downloadedHash = TransferFsImage.downloadImageToStorage(
                  nnHostPort, sig.mostRecentCheckpointTxId, dstImage.getStorage(), true);
              dstImage.saveDigestAndRenameCheckpointImage(
                  sig.mostRecentCheckpointTxId, downloadedHash);
            }
        
            // get edits file
            for (RemoteEditLog log : manifest.getLogs()) {
              TransferFsImage.downloadEditsToStorage(
                  nnHostPort, log, dstImage.getStorage());
            }
        
            return Boolean.valueOf(downloadImage);
          }
        });
        return b.booleanValue();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
  }
  
  InetSocketAddress getNameNodeAddress() {
    return nameNodeAddr;
  }

  /**
   * Copy the new fsimage into the NameNode
   */
  private void putFSImage(CheckpointSignature sig, long txid) throws IOException {
    String fileid = "putimage=1&txid=" + txid + "&port=" + imagePort +
      "&machine=" + infoBindAddress + 
      "&token=" + sig.toString();
    LOG.info("Posted URL " + fsName + fileid);
    TransferFsImage.getFileClient(fsName, fileid, null, false);
  }

  /**
   * Returns the Jetty server that the Namenode is listening on.
   */
  private String getInfoServer() throws IOException {
    URI fsName = FileSystem.getDefaultUri(conf);
    if (!FSConstants.HDFS_URI_SCHEME.equalsIgnoreCase(fsName.getScheme())) {
      throw new IOException("This is not a DFS");
    }

    String configuredAddress = DFSUtil.getInfoServer(null, conf, true);
    InetSocketAddress sockAddr = NetUtils.createSocketAddr(configuredAddress);
    if (sockAddr.getAddress().isAnyLocalAddress()) {
      if(UserGroupInformation.isSecurityEnabled()) {
        throw new IOException("Cannot use a wildcard address with security. " +
                              "Must explicitly set bind address for Kerberos");
      }
      return fsName.getHost() + ":" + sockAddr.getPort();
    } else {
      if(LOG.isDebugEnabled()) {
        LOG.debug("configuredAddress = " + configuredAddress);
      }
      return configuredAddress;
    }
  }

  /**
   * Create a new checkpoint
   * @return if the image is fetched from primary or not
   */
  boolean doCheckpoint() throws IOException {
    checkpointImage.ensureCurrentDirExists();
    
    // Tell the namenode to start logging transactions in a new edit file
    // Returns a token that would be used to upload the merged image.
    CheckpointSignature sig = namenode.rollEditLog();

    // error simulation code for junit test
    if (ErrorSimulator.getErrorSimulation(0)) {
      throw new IOException("Simulating error0 " +
                            "after creating edits.new");
    }

    RemoteEditLogManifest manifest =
      namenode.getEditLogManifest(sig.mostRecentCheckpointTxId + 1);

    // Sanity check manifest - these could happen if, eg, someone on the
    // NN side accidentally rmed the storage directories
    if (manifest.getLogs().isEmpty()) {
      throw new IOException("Found no edit logs to download on NN since txid " 
          + sig.mostRecentCheckpointTxId);
    }
    if (manifest.getLogs().get(0).getStartTxId() != sig.mostRecentCheckpointTxId + 1) {
      throw new IOException("Bad edit log manifest (expected txid = " +
          (sig.mostRecentCheckpointTxId + 1) + ": " + manifest);
    }
    
    boolean loadImage = downloadCheckpointFiles(
        fsName, checkpointImage, sig, manifest);   // Fetch fsimage and edits
    doMerge(conf, sig, manifest, loadImage, checkpointImage);
    
    //
    // Upload the new image into the NameNode. Then tell the Namenode
    // to make this new uploaded image as the most current image.
    //
    long txid = checkpointImage.getStorage().getMostRecentCheckpointTxId();
    putFSImage(sig, txid);

    // error simulation code for junit test
    if (ErrorSimulator.getErrorSimulation(1)) {
      throw new IOException("Simulating error1 " +
                            "after uploading new image to NameNode");
    }

    LOG.warn("Checkpoint done. New Image Size: " 
             + checkpointImage.getStorage().getFsImageName(txid).length());
    
    // Since we've successfully checkpointed, we can remove some old
    // image files
    checkpointImage.getStorage().archiveOldStorage();
    
    return loadImage;
  }

  /**
   * @param argv The parameters passed to this program.
   * @exception Exception if the filesystem does not exist.
   * @return 0 on success, non zero on error.
   */
  private int processArgs(String[] argv) throws Exception {

    if (argv.length < 1) {
      printUsage("");
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = argv[i++];

    //
    // verify that we have enough command line parameters
    //
    if ("-geteditsize".equals(cmd)) {
      if (argv.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    } else if ("-checkpoint".equals(cmd)) {
      if (argv.length != 1 && argv.length != 2) {
        printUsage(cmd);
        return exitCode;
      }
      if (argv.length == 2 && !"force".equals(argv[i])) {
        printUsage(cmd);
        return exitCode;
      }
    }

    exitCode = 0;
    try {
      if ("-checkpoint".equals(cmd)) {
        long size = namenode.getEditLogSize();
        if (size >= checkpointSize || 
            argv.length == 2 && "force".equals(argv[i])) {
          doCheckpoint();
        } else {
          System.err.println("EditLog size " + size + " bytes is " +
                             "smaller than configured checkpoint " +
                             "size " + checkpointSize + " bytes.");
          System.err.println("Skipping checkpoint.");
        }
      } else if ("-geteditsize".equals(cmd)) {
        long size = namenode.getEditLogSize();
        System.out.println("EditLog size is " + size + " bytes");
      } else {
        exitCode = -1;
        LOG.error(cmd.substring(1) + ": Unknown command");
        printUsage("");
      }
    } catch (RemoteException e) {
      //
      // This is a error returned by hadoop server. Print
      // out the first line of the error mesage, ignore the stack trace.
      exitCode = -1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        LOG.error(cmd.substring(1) + ": "
                  + content[0]);
      } catch (Exception ex) {
        LOG.error(cmd.substring(1) + ": "
                  + ex.getLocalizedMessage());
      }
    } catch (IOException e) {
      //
      // IO exception encountered locally.
      //
      exitCode = -1;
      LOG.error(cmd.substring(1) + ": "
                + e.getLocalizedMessage());
    } finally {
      // Does the RPC connection need to be closed?
    }
    return exitCode;
  }

  /**
   * Displays format of commands.
   * @param cmd The command that is being executed.
   */
  private void printUsage(String cmd) {
    if ("-geteditsize".equals(cmd)) {
      System.err.println("Usage: java SecondaryNameNode"
                         + " [-geteditsize]");
    } else if ("-checkpoint".equals(cmd)) {
      System.err.println("Usage: java SecondaryNameNode"
                         + " [-checkpoint [force]]");
    } else {
      System.err.println("Usage: java SecondaryNameNode " +
                         "[-checkpoint [force]] " +
                         "[-geteditsize] ");
    }
  }

  /**
   * main() has some simple utility methods.
   * @param argv Command line parameters.
   * @exception Exception if the filesystem does not exist.
   */
  public static void main(String[] argv) throws Exception {
    StringUtils.startupShutdownMessage(SecondaryNameNode.class, argv, LOG);
    Configuration tconf = new HdfsConfiguration();
    if (argv.length >= 1) {
      SecondaryNameNode secondary = new SecondaryNameNode(tconf);
      int ret = secondary.processArgs(argv);
      System.exit(ret);
    }

    // Create a never ending deamon
    Daemon checkpointThread = new Daemon(new SecondaryNameNode(tconf)); 
    checkpointThread.start();
  }

  static class CheckpointStorage extends FSImage {
    /**
     * Construct a checkpoint image.
     * @param conf Node configuration.
     * @param imageDirs URIs of storage for image.
     * @param editDirs URIs of storage for edit logs.
     * @throws IOException If storage cannot be access.
     */
    CheckpointStorage(Configuration conf, 
                      Collection<URI> imageDirs,
                      Collection<URI> editsDirs) throws IOException {
      super(conf, (FSNamesystem)null, imageDirs, editsDirs);
    }

    /**
     * Analyze checkpoint directories.
     * Create directories if they do not exist.
     * Recover from an unsuccessful checkpoint is necessary.
     *
     * @throws IOException
     */
    void recoverCreate() throws IOException {
      storage.attemptRestoreRemovedStorage();
      storage.unlockAll();

      for (Iterator<StorageDirectory> it = 
                   storage.dirIterator(); it.hasNext();) {
        StorageDirectory sd = it.next();
        boolean isAccessible = true;
        try { // create directories if don't exist yet
          if(!sd.getRoot().mkdirs()) {
            // do nothing, directory is already created
          }
        } catch(SecurityException se) {
          isAccessible = false;
        }
        if(!isAccessible)
          throw new InconsistentFSStateException(sd.getRoot(),
              "cannot access checkpoint directory.");
        StorageState curState;
        try {
          curState = sd.analyzeStorage(HdfsConstants.StartupOption.REGULAR);
          // sd is locked but not opened
          switch(curState) {
          case NON_EXISTENT:
            // fail if any of the configured checkpoint dirs are inaccessible 
            throw new InconsistentFSStateException(sd.getRoot(),
                  "checkpoint directory does not exist or is not accessible.");
          case NOT_FORMATTED:
            break;  // it's ok since initially there is no current and VERSION
          case NORMAL:
            break;
          default:  // recovery is possible
            sd.doRecover(curState);
          }
        } catch (IOException ioe) {
          sd.unlock();
          throw ioe;
        }
      }
    }
    
    /**
     * Ensure that the current/ directory exists in all storage
     * directories
     */
    void ensureCurrentDirExists() throws IOException {
      for (Iterator<StorageDirectory> it
             = storage.dirIterator(); it.hasNext();) {
        StorageDirectory sd = it.next();
        File curDir = sd.getCurrentDir();
        if (!curDir.exists() && !curDir.mkdirs()) {
          throw new IOException("Could not create directory " + curDir);
        }
      }
    }
  }
    
  static void doMerge(Configuration conf, 
      CheckpointSignature sig, RemoteEditLogManifest manifest,
      boolean loadImage, FSImage dstImage) throws IOException {   
    NNStorage dstStorage = dstImage.getStorage();
    
    dstStorage.setStorageInfo(sig);
    if (loadImage) {
      // TODO: dstImage.namesystem.close(); ??
      dstImage.namesystem = new FSNamesystem(dstImage, conf);
      dstImage.editLog = new FSEditLog(dstStorage);

      File file = dstStorage.findImageFile(sig.mostRecentCheckpointTxId);
      if (file == null) {
        throw new IOException("Couldn't find image file at txid " + 
            sig.mostRecentCheckpointTxId + " even though it should have " +
            "just been downloaded");
      }
      LOG.debug("2NN loading image from " + file);
      dstImage.loadFSImage(file);
    }
    
    long sinceTxId = sig.mostRecentCheckpointTxId + 1;

    dstImage.loadEdits(); 
    
    // TODO: why do we need the following two lines? We shouldn't have even
    // been able to download an image from a NN that had a different
    // cluster ID or blockpool ID! this should only be done for the
    // very first checkpoint.
    dstStorage.setClusterID(sig.getClusterID());
    dstStorage.setBlockPoolID(sig.getBlockpoolID());
    
    sig.validateStorageInfo(dstImage);
    dstImage.saveFSImageInAllDirs(dstImage.getEditLog().getLastWrittenTxId());
    dstStorage.writeAll();
  }
}
