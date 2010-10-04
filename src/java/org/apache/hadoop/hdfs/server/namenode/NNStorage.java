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
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
//import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.JournalStream.JournalType;
import org.apache.hadoop.conf.Configuration;

public class NNStorage extends Storage implements Iterable{

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
	
	////////////////////////////////////////////////////////////////////////	
	// TODO
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
	
	public NNStorage(Configuration conf){
		super(NodeType.NAME_NODE);
		this.conf = conf;
		
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
	
	
	
	
	
	/////////////////////////////////////////////////////////////////////
	// PUBLIC API, but not so sure about them
	/////////////////////////////////////////////////////////////////////
	
	/**
	 * Return number of storage directories of the given type.
	 * 
	 * @param dirType
	 *            directory type
	 * @return number of storage directories of type dirType
	 */
	public int getNumStorageDirs(NameNodeDirType dirType) {
		if (dirType == null)
			return getNumStorageDirs();
		Iterator<StorageDirectory> it = dirIterator(dirType);
		int numDirs = 0;
		for (; it.hasNext(); it.next())
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
	}
	
	
	public File getEditFile(StorageDirectory sd) {
		return getImageFile(sd, NameNodeFile.EDITS);
	}
	
	public File getEditNewFile(StorageDirectory sd) {
		return getImageFile(sd, NameNodeFile.EDITS_NEW);
	}
	
	
	///////////////////////////////////////////////////////////////////////
	// PRIVATE methods
	///////////////////////////////////////////////////////////////////////
	
	
	static protected File getImageFile(StorageDirectory sd, NameNodeFile type) {
		return new File(sd.getCurrentDir(), type.getName());
	}

	  
	// TODO
	void setStorageDirectories(Collection<URI> fsNameDirs,
	                            Collection<URI> fsEditsDirs) throws IOException {
		
		this.storageDirs = new ArrayList<StorageDirectory>();
	    this.removedStorageDirs = new ArrayList<StorageDirectory>();
	    
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
	      checkSchemeConsistency(dirName);
	      // Add to the list of storage directories, only if the 
	      // URI is of type file://
	      if(dirName.getScheme().compareTo(JournalType.FILE.name().toLowerCase())
	          == 0)
	        this.addStorageDir(new StorageDirectory(new File(dirName.getPath()), 
	                    NameNodeDirType.EDITS));
	    }
	 }


	// TODO
	synchronized void attemptRestoreRemovedStorage() {
		// if directory is "alive" - copy the images there...
		if (!restoreFailedStorage || removedStorageDirs.size() == 0)
			return; // nothing to restore

		LOG.info("FSImage.attemptRestoreRemovedStorage: check removed(failed) "
				+ "storarge. removedStorages size = "
				+ removedStorageDirs.size());
		for (Iterator<StorageDirectory> it = this.removedStorageDirs.iterator(); it
				.hasNext();) {
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
		}
	}


	
	
	
	
	
}
