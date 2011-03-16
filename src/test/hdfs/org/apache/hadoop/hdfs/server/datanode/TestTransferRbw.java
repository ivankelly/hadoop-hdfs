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
package org.apache.hadoop.hdfs.server.datanode;

import java.util.Collection;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;
import org.junit.Assert;
import org.junit.Test;

/** Test transferring RBW between datanodes */
public class TestTransferRbw {
  private static final Log LOG = LogFactory.getLog(TestTransferRbw.class);

  private static final Random RAN = new Random();
  private static final short REPLICATION = (short)1;

  private static ReplicaBeingWritten getRbw(final DataNode datanode
      ) throws InterruptedException {
    return (ReplicaBeingWritten)getReplica(datanode, ReplicaState.RBW);
  }
  private static ReplicaInPipeline getReplica(final DataNode datanode,
      final ReplicaState expectedState) throws InterruptedException {
    final FSDataset dataset = ((FSDataset)datanode.data);
    final Collection<ReplicaInfo> replicas = dataset.volumeMap.replicas();
    for(int i = 0; i < 5 && replicas.size() == 0; i++) {
      LOG.info("wait since replicas.size() == 0; i=" + i);
      Thread.sleep(1000);
    }
    Assert.assertEquals(1, replicas.size());
    final ReplicaInfo r = replicas.iterator().next();
    Assert.assertEquals(expectedState, r.getState());
    return (ReplicaInPipeline)r;
  }

  @Test
  public void testTransferRbw() throws Exception {
    final HdfsConfiguration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf
        ).numDataNodes(REPLICATION).build();
    try {
      cluster.waitActive();
      final DistributedFileSystem fs = (DistributedFileSystem)cluster.getFileSystem();

      //create a file, write some data and leave it open. 
      final Path p = new Path("/foo");
      final int size = (1 << 16) + RAN.nextInt(1 << 16);
      LOG.info("size = " + size);
      final FSDataOutputStream out = fs.create(p, REPLICATION);
      final byte[] bytes = new byte[1024];
      for(int remaining = size; remaining > 0; ) {
        RAN.nextBytes(bytes);
        final int len = bytes.length < remaining? bytes.length: remaining;
        out.write(bytes, 0, len);
        out.hflush();
        remaining -= len;
      }

      //get the RBW
      final ReplicaBeingWritten oldrbw;
      final DataNode newnode;
      final DatanodeInfo newnodeinfo;
      final long visible;
      {
        final DataNode oldnode = cluster.getDataNodes().get(0);
        oldrbw = getRbw(oldnode);
        LOG.info("oldrbw = " + oldrbw);
        
        //add a datanode
        cluster.startDataNodes(conf, 1, true, null, null);
        newnode = cluster.getDataNodes().get(REPLICATION);
        
        {
          final DatanodeInfo[] datatnodeinfos = cluster.getNameNode(
              ).getDatanodeReport(DatanodeReportType.LIVE);
          Assert.assertEquals(2, datatnodeinfos.length);
          int i = 0;
          for(; i < datatnodeinfos.length
                && !datatnodeinfos[i].equals(newnode.dnRegistration); i++);
          Assert.assertTrue(i < datatnodeinfos.length);
          newnodeinfo = datatnodeinfos[i];
        }
        
        //transfer RBW
        visible = oldnode.transferBlockForPipelineRecovery(oldrbw, new DatanodeInfo[]{newnodeinfo});
      }

      //check temporary
      final ReplicaInPipeline temp = getReplica(newnode, ReplicaState.TEMPORARY);
      LOG.info("temp = " + temp);
      Assert.assertEquals(oldrbw.getBlockId(), temp.getBlockId());
      Assert.assertEquals(oldrbw.getGenerationStamp(), temp.getGenerationStamp());
      final Block b = new Block(oldrbw.getBlockId(), visible, oldrbw.getGenerationStamp());
      //convert temporary to rbw
      newnode.convertTemporaryToRbw(b);
      //check new rbw
      final ReplicaBeingWritten newrbw = getRbw(newnode);
      LOG.info("newrbw = " + newrbw);
      Assert.assertEquals(oldrbw.getBlockId(), newrbw.getBlockId());
      Assert.assertEquals(oldrbw.getGenerationStamp(), newrbw.getGenerationStamp());
      Assert.assertEquals(oldrbw.getVisibleLength(), newrbw.getVisibleLength());

      LOG.info("DONE");
    } finally {
      cluster.shutdown();
    }
  }
}
