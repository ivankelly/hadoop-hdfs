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

import static org.junit.Assert.*;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.FSImageTransactionalStorageInspector.FoundFSImage;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFSImageStorageInspector {
  private static final Log LOG = LogFactory.getLog(
      TestFSImageStorageInspector.class);

  /**
   * Simple test with image, edits, and inprogress edits
   */
  @Test
  public void testCurrentStorageInspector() throws IOException {
    FSImageTransactionalStorageInspector inspector = 
      new FSImageTransactionalStorageInspector();
    
    StorageDirectory mockDir = mockDirectory(
        NameNodeDirType.IMAGE_AND_EDITS,
        false,
        "/foo/current/fsimage_123",
        "/foo/current/edits_123-456",
        "/foo/current/fsimage_456",
        "/foo/current/edits_inprogress_457");

    inspector.inspectDirectory(mockDir);
    
    assertEquals(2, inspector.foundImages.size());
    
    FoundFSImage latestImage = inspector.getLatestImage();
    assertEquals(456, latestImage.txId);
    assertSame(mockDir, latestImage.sd);
    assertTrue(inspector.isUpgradeFinalized());
    
    assertEquals(new File("/foo/current/fsimage_456"), 
                 inspector.getImageFileForLoading());
  }
  
  // /**
  //  * Test case for the usual case where no recovery of a log group is necessary
  //  * (i.e all logs have the same start and end txids and finalized)
  //  */
  // @Test
  // public void testLogGroupRecoveryNoop() throws IOException {
  //   FSImageTransactionalStorageInspector inspector =
  //       new FSImageTransactionalStorageInspector(new NNStorage(new Configuration(),
  //                                                              Collections.<URI>emptyList(),
  //                                                              Collections.<URI>emptyList()));

  //   inspector.inspectImageDirectory(
  //       mockDirectoryWithEditLogs("/foo1/current/edits_123-456"));
  //   inspector.inspectImageDirectory(
  //       mockDirectoryWithEditLogs("/foo2/current/edits_123-456"));
  //   inspector.inspectImageDirectory(
  //       mockDirectoryWithEditLogs("/foo3/current/edits_123-456"));

  //   LogGroup lg = inspector.logGroups.get(123L);
  //   assertEquals(3, lg.logs.size());
    
  //   lg.planRecovery();
    
  //   assertFalse(lg.logs.get(0).isCorrupt());
  //   assertFalse(lg.logs.get(1).isCorrupt());
  //   assertFalse(lg.logs.get(2).isCorrupt());
  // }
  
  // /**
  //  * Test case where we have some in-progress and some finalized logs
  //  * for a given txid.
  //  */
  // @Test
  // public void testLogGroupRecoveryMixed() throws IOException {
  //   FSImageTransactionalStorageInspector inspector =
  //       new FSImageTransactionalStorageInspector(new NNStorage(new Configuration(),
  //                                                              Collections.<URI>emptyList(),
  //                                                              Collections.<URI>emptyList()));

  //   inspector.inspectImageDirectory(
  //       mockDirectoryWithEditLogs("/foo1/current/edits_123-456"));
  //   inspector.inspectImageDirectory(
  //       mockDirectoryWithEditLogs("/foo2/current/edits_123-456"));
  //   inspector.inspectImageDirectory(
  //       mockDirectoryWithEditLogs("/foo3/current/edits_inprogress_123"));
  //   inspector.inspectImageDirectory(mockDirectory(
  //       NameNodeDirType.IMAGE,
  //       false,
  //       "/foo4/current/fsimage_122"));

  //   LogGroup lg = inspector.logGroups.get(123L);
  //   assertEquals(3, lg.logs.size());
  //   FoundEditLog inProgressLog = lg.logs.get(2);
  //   assertTrue(inProgressLog.isInProgress());
    
  //   LoadPlan plan = inspector.createLoadPlan();

  //   // Check that it was marked corrupt.
  //   assertFalse(lg.logs.get(0).isCorrupt());
  //   assertFalse(lg.logs.get(1).isCorrupt());
  //   assertTrue(lg.logs.get(2).isCorrupt());

    
  //   // Calling recover should move it aside
  //   inProgressLog = spy(inProgressLog);
  //   Mockito.doNothing().when(inProgressLog).moveAsideCorruptFile();
  //   lg.logs.set(2, inProgressLog);
    
  //   plan.doRecovery();
    
  //   Mockito.verify(inProgressLog).moveAsideCorruptFile();
  // }
  
//   @Test
//   public void testLogManifest() throws IOException { 
//     FSImageTransactionalStorageInspector inspector =
//         new FSImageTransactionalStorageInspector(new NNStorage(new Configuration(),
//                                                                Collections.<URI>emptyList(),
//                                                                Collections.<URI>emptyList()));
//     inspector.inspectImageDirectory(
//         mockDirectoryWithEditLogs("/foo1/current/edits_1-1",
//                                   "/foo1/current/edits_2-200"));
//     inspector.inspectDirectory(
//         mockDirectoryWithEditLogs("/foo2/current/edits_inprogress_1",
//                                   "/foo2/current/edits_201-400"));
//     inspector.inspectImageDirectory(
//         mockDirectoryWithEditLogs("/foo3/current/edits_1-1",
//                                   "/foo3/current/edits_2-200"));
    
//     assertEquals("[[1,1], [2,200], [201,400]]",
//                  inspector.getEditLogManifest(1).toString());
//     assertEquals("[[2,200], [201,400]]",
//                  inspector.getEditLogManifest(2).toString());
//     assertEquals("[[2,200], [201,400]]",
//                  inspector.getEditLogManifest(10).toString());
//     assertEquals("[[201,400]]",
//                  inspector.getEditLogManifest(201).toString());
//   }  

//   /**
//    * Test case where an in-progress log is in an earlier name directory
//    * than a finalized log. Previously, getEditLogManifest wouldn't
//    * see this log.
//    */
//   @Test
//   public void testLogManifestInProgressComesFirst() throws IOException { 
//     FSImageTransactionalStorageInspector inspector =
//         new FSImageTransactionalStorageInspector();
//     inspector.inspectDirectory(
//         mockDirectoryWithEditLogs("/foo1/current/edits_2622-2623",
//                                   "/foo1/current/edits_2624-2625",
//                                   "/foo1/current/edits_inprogress_2626"));
//     inspector.inspectDirectory(
//         mockDirectoryWithEditLogs("/foo2/current/edits_2622-2623",
//                                   "/foo2/current/edits_2624-2625",
//                                   "/foo2/current/edits_2626-2627",
//                                   "/foo2/current/edits_2628-2629"));
    
//     assertEquals("[[2622,2623], [2624,2625], [2626,2627], [2628,2629]]",
//                  inspector.getEditLogManifest(2621).toString());
//   }  
  
//   private StorageDirectory mockDirectoryWithEditLogs(String... fileNames) {
//     return mockDirectory(NameNodeDirType.EDITS, false, fileNames);
//   }
  
  /**
   * Make a mock storage directory that returns some set of file contents.
   * @param type type of storage dir
   * @param previousExists should we mock that the previous/ dir exists?
   * @param fileNames the names of files contained in current/
   */
  static StorageDirectory mockDirectory(
      StorageDirType type,
      boolean previousExists,
      String...  fileNames) {
    StorageDirectory sd = mock(StorageDirectory.class);
    
    doReturn(type).when(sd).getStorageDirType();

    // Version file should always exist
    doReturn(FSImageTestUtil.mockFile(true)).when(sd).getVersionFile();
    
    // Previous dir optionally exists
    doReturn(FSImageTestUtil.mockFile(previousExists))
      .when(sd).getPreviousDir();   

    // Return a mock 'current' directory which has the given paths
    File[] files = new File[fileNames.length];
    for (int i = 0; i < fileNames.length; i++) {
      files[i] = new File(fileNames[i]);
    }
    
    File mockDir = Mockito.spy(new File("/dir/current"));
    doReturn(files).when(mockDir).listFiles();
    doReturn(mockDir).when(sd).getCurrentDir();
    
    return sd;
  }
}
