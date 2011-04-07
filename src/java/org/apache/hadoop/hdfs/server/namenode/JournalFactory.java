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
import java.net.URI;
import java.util.List;
import java.io.IOException;

/**
 * Factory for creation of edit log input and output streams.
 * Third partys can implement this class to provide customised
 * edit logging
 */
abstract class JournalFactory {
  private Configuration conf;
  private URI uri;
  private boolean openness;

  /** 
   * Construct the factory
   * @param conf Configuration
   * @param uri  URI which identifies the editlog in the system
   * @throw IllegalArgumentException if factory cannot be initialised
   */
  public JournalFactory(Configuration conf, URI uri)
      throws IllegalArgumentException {
    this.conf = conf;
    this.uri = uri;
    openness = false;
  }

  protected Configuration getConfiguration() {
    return conf;
  }
  
  /**
   * Format the edit log area, specified by the factory's URI
   */
  abstract void format () throws IOException;

  
  // // these will be replaced by a single roll() after 1073
  // // or possibly nothing at all
  // /** 
  //  * Start rolling.
  //  */
  // void startRoll() throws IOException {}
  // /**
  //  * @return true if factory is currently rolli
  // boolean isRolling() { return false; }
  // void endRoll() throws IOException {}
  
  // Streams
  abstract EditLogInputStream getInputStream()
      throws IOException;
  abstract EditLogOutputStream getOutputStream()
      throws IOException;
}