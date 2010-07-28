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

package org.honu.datacollection.writer.localfs;

import java.io.FileNotFoundException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class LocalToRemoteHdfsMover extends Thread {
  static Logger log = Logger.getLogger(LocalToRemoteHdfsMover.class);
  static SimpleDateFormat day = new java.text.SimpleDateFormat("yyyyMMdd'/'");
  
  static {
		// Force GMT
		day.setTimeZone(TimeZone.getTimeZone("GMT"));
  }
  
  private FileSystem remoteFs = null;
  private FileSystem localFs = null;
  private Configuration conf = null;
  private String group = null;
  private String fsname = null;
  private String localOutputDir = null;
  private String remoteOutputDir = null;
  private boolean exitIfHDFSNotavailable = false;
  private boolean appendDateToOutputDir = false;
  private BlockingQueue<String> fileQueue = null;
  private volatile boolean isRunning = true;
  private boolean isS3FileSystem = false;
  
  public LocalToRemoteHdfsMover(BlockingQueue<String> fileQueue ,Configuration conf) {
    this("defaultGroup",fileQueue,conf);
  }

  public LocalToRemoteHdfsMover(String group,BlockingQueue<String> fileQueue ,Configuration conf) {
    this.group = group;
    this.fileQueue = fileQueue;
    this.conf = conf;
    this.setDaemon(true);
    this.setName("LocalToRemoteHdfsMover-" + group);
    this.start();
  }
  protected void init() throws Throwable {

    // check if they've told us the file system to use
    
    fsname = conf.get("honu.collector." + group + ".writer.hdfs.filesystem");
    if (fsname == null || fsname.equals("")) {
      // otherwise try to get the filesystem from hadoop
      fsname = conf.get("fs.default.name");
    }

    if (fsname == null) {
      log.error("no filesystem name");
      throw new RuntimeException("no filesystem");
    }

    exitIfHDFSNotavailable = conf.getBoolean(
        "honu.collector." + group + ".exitIfHDFSNotavailable", false);

    remoteFs = FileSystem.get(new URI(fsname), conf);
    if (remoteFs == null && exitIfHDFSNotavailable) {
      log.error( group + "- can't connect to HDFS at " + remoteFs.getUri() + " bail out!");
      DaemonWatcher.bailout(-1);
    } 
    
    if (remoteFs.getUri().toASCIIString().toLowerCase().startsWith("s3n")) {
    	isS3FileSystem = true;
    	log.info(group + "- S3n FileSystem detected, will not use rename on S3");
    }
    
    appendDateToOutputDir = conf.getBoolean("honu.collector." + group + ".appendDateToOutputDir", false);
    
    localFs = FileSystem.getLocal(conf);
    
    remoteOutputDir = conf.get("honu.collector." + group +".outputDir", "/honu/logs/");
    if (!remoteOutputDir.endsWith("/")) {
      remoteOutputDir += "/";
    }
    
    localOutputDir = conf.get("honu.collector." + group +".localOutputDir",
    "/honu/datasink/");
    if (!localOutputDir.endsWith("/")) {
      localOutputDir += "/";
    }
   
    log.info(group +"- remoteFs: " + remoteFs.getUri().toASCIIString());
    log.info(group +"- appendDateToOutputDir:" + appendDateToOutputDir);
    log.info(group +"- localOutputDir:" + localOutputDir);
    log.info(group +"- remoteOutputDir:" + remoteOutputDir);

  }

  protected void moveFile(String filePath) throws Exception{
    String remoteFilePath = filePath.substring(filePath.lastIndexOf("/")+1,filePath.lastIndexOf("."));
    if (appendDateToOutputDir) {
    	 remoteFilePath = remoteOutputDir +  day.format(new Date())  + remoteFilePath;
    } else {
    	 remoteFilePath = remoteOutputDir + remoteFilePath;
    }
   
    try {
    
      Path pLocalPath = new Path(filePath);
      // Check for invalid DataSink file
      // This could happen in case of IOException or kill -9
      FileStatus fileStatus = localFs.getFileStatus(pLocalPath);
      if ( fileStatus.getLen() == 0) {
        localFs.delete(pLocalPath,false);
        log.warn( group + "- Invalid DataSink file: len=0 - deleting from local: " + pLocalPath);
        return;
      }
    
    	if (isS3FileSystem) {		
        Path pRemoteFilePath = new Path(remoteFilePath + ".done");
      	// Don't do a rename on S3 since rename is NOT atomic
        // OK since it should throw an IOException
        // and HTTP PUT is atomic on S3
        remoteFs.copyFromLocalFile(false, true, pLocalPath, pRemoteFilePath);
        localFs.delete(pLocalPath,false);
        log.info( group + "- copy done deleting from local: " + pLocalPath);
        
    	} else {
        Path pRemoteFilePath = new Path(remoteFilePath + ".chukwa");
        remoteFs.copyFromLocalFile(false, true, pLocalPath, pRemoteFilePath);
        Path pFinalRemoteFilePath = new Path(remoteFilePath + ".done");
        if ( remoteFs.rename(pRemoteFilePath, pFinalRemoteFilePath)) {
          localFs.delete(pLocalPath,false);
          log.info( group + "- move done deleting from local: " + pLocalPath);
        } else {
          throw new RuntimeException("Cannot rename remote file, " + pRemoteFilePath + " to " + pFinalRemoteFilePath);
        }    		
    	}

    
    }catch(FileNotFoundException ex) {
      //do nothing since if the file is no longer there it's
      // because it has already been moved over by the cleanup task.
    }
    catch (Exception e) {
      log.warn( group + "- Cannot copy to the remote HDFS",e);
      throw e;
    }
  }
  
  protected void cleanup() throws Exception{
    try {
      int rotateInterval = conf.getInt("honu.collector." + group +"rotateInterval",
          1000 * 60 * 5);// defaults to 5 minutes
      
      Path pLocalOutputDir = new Path(localOutputDir);
      FileStatus[] files = localFs.listStatus(pLocalOutputDir);
      String fileName = null;
      for (FileStatus file: files) {
        fileName = file.getPath().getName();
        if (fileName.endsWith(".done")) {
          moveFile(localOutputDir + fileName);
        } else if (fileName.endsWith(".chukwa")) {
          long lastPeriod = System.currentTimeMillis() - rotateInterval - (2*60*1000);
          if (file.getModificationTime() < lastPeriod) {
            log.info( group + "- Moving .chukwa file over, " + localOutputDir + fileName);
            moveFile(localOutputDir + fileName);
          }
        }
      }
    }catch (Exception e) {
      log.warn("Cannot copy to the remote HDFS",e);
      throw e;
    }
  }
  
  @Override
  public void run() {
    boolean inError = true;
    String filePath = null;
    
    while (isRunning) {
      try {
        if (inError) {
          init();
          cleanup();
          inError = false;
        }

        filePath = fileQueue.take();
        
        if (filePath == null) {
          continue;
        }
        
        moveFile(filePath);
        cleanup();
        filePath = null;
        
      } catch (Throwable e) {
        log.warn( group + "- Error in LocalToHdfsMover", e);
        inError = true;
        try {
          log.info(group + "- Got an exception going to sleep for 60 secs");
          Thread.sleep(60000);
        } catch (Throwable e2) {
          log.warn( group + "- Exception while sleeping", e2);
        }
      }
    }
    log.info(Thread.currentThread().getName() + " is exiting.");
  }

  public void shutdown() {
    this.isRunning = false;
  }
}
