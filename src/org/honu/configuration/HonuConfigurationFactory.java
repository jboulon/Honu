/*
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

package org.honu.configuration;


import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class HonuConfigurationFactory {
  static Logger log = Logger.getLogger(HonuConfigurationFactory.class);

  static Object lock = new Object();
  
  protected static HonuConfigurationFactory honuConfigurationFactory = null;
  protected Configuration collectorConfiguration = null;
  protected Configuration demuxConfiguration = null;
  protected String honuConf = null;
  
  private HonuConfigurationFactory() {
    String honuHome = System.getenv("HONU_HOME");
    if (honuHome == null) {
      honuHome = ".";
    }

    if (!honuHome.endsWith("/")) {
      honuHome = honuHome + File.separator;
    }
    
    honuConf = System.getenv("HONU_CONF_DIR");
    if (honuConf == null) {
      honuConf = honuHome + "conf" + File.separator;
    }

    log.info("Honu configuration is using " + honuConf);
  }

  public static HonuConfigurationFactory getInstance() {
    synchronized(lock) {
      if ( honuConfigurationFactory == null ) {
        honuConfigurationFactory = new HonuConfigurationFactory();
      }
    }
    return honuConfigurationFactory;
  }
  
  public Configuration getCollectorConfiguration() {
    synchronized(lock) {
      if (collectorConfiguration == null) {
        collectorConfiguration = new Configuration();
        collectorConfiguration.addResource(new Path(honuConf + "/honu-collector-conf.xml"));
      }
    }
    return collectorConfiguration;
  }
  public Configuration getDemuxConfiguration() {
    synchronized(lock) {
      if (demuxConfiguration == null) {
        demuxConfiguration = new Configuration();
        demuxConfiguration.addResource(new Path(honuConf + "/honu-demux-conf.xml"));
      }
    }
    return demuxConfiguration;
  }
  
}
