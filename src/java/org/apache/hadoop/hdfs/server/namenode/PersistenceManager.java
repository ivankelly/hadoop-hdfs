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

import org.apache.hadoop.conf.Configuration;



public class PersistenceManager {

	
	private FSImage fsi;
	private FSEditLog fse;
	private NNStorage storage;
	
	/* Constructor */
	public PersistenceManager(Configuration conf){
		
		storage = new NNStorage(conf);
		fsi = new FSImage(conf,storage);
		
		
	}
	
	
	
	
	/**
	For managing checkpoints
	*/
	public void startCheckpoint();
	public void endCheckpoint();
	/**
	Implement functionallity used by the command line options
	*/
	public static void upgrade();
	public static void rollback();
	public static void finalize();
	public static void format();
	/**
	Save the contents of FSNameSystem to disk
	Fundamentally just dumps mem and resets the edit log
	*/
	public void save();
	/**
	Load the latest version of the FSNameSystem from disk
	w Interfaces
	*/
	public void load();
	
	FSEditLog getLog();

	
	
	
}
