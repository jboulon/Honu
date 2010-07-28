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


import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter;
import org.apache.hadoop.chukwa.datacollection.writer.WriterException;
import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.honu.util.Tracer;

public class LockFreeWriter extends Thread implements ChukwaWriter {

	static Logger log = Logger.getLogger(LockFreeWriter.class);
	static final int STAT_INTERVAL_SECONDS = 30;
	static String localHostAddr = null;
	
	 
	private BlockingQueue<Chunk> ChunkQueue = null;
	private SimpleDateFormat day = new java.text.SimpleDateFormat("'P'yyyyMMdd'T'HHmmss");
	
	private BlockingQueue<String> fileQueue = null;
	private LocalToRemoteHdfsMover localToRemoteHdfsMover = null;
	private FileSystem fs = null;
	private Configuration conf = null;

	private String localOutputDir = null;
	private Calendar calendar = Calendar.getInstance();

	private Path currentPath = null;
	private String currentFileName = null;
	private FSDataOutputStream currentOutputStr = null;
	private SequenceFile.Writer seqFileWriter = null;
	private CompressionCodec codec = null;
	
	private int rotateInterval = 1000 * 60;


	private volatile long dataSize = 0;
	private volatile boolean isRunning = true;

	private Timer rotateTimer = null;
	private Timer statTimer = null;


	private String group = null;
	private int initWriteChunkRetries = 10;
	private int writeChunkRetries = initWriteChunkRetries;
	private boolean chunksWrittenThisRotate = false;
	private volatile boolean isStopped = false;
	private volatile long timeOut = Long.MAX_VALUE -1;
	
	private long timePeriod = -1;
	private long nextTimePeriodComputation = -1;
	private long nextRotate = -1;
	private int minPercentFreeDisk = 20;
	
	static {
		try {
			localHostAddr = "_" + InetAddress.getLocalHost().getHostName() + "_";
		} catch (UnknownHostException e) {
			localHostAddr = "-NA-";
		}
	}

	public LockFreeWriter(String group,BlockingQueue<Chunk> ChunkQueue) {
	  this.group = group;
		this.ChunkQueue = ChunkQueue;
		this.setName("LockFreeWriter-" + group);
	}

	@SuppressWarnings("unchecked")
  public void init(Configuration conf) throws WriterException {
		this.conf = conf;
    // Force GMT
    day.setTimeZone(TimeZone.getTimeZone("GMT"));
    
		try {
			fs = FileSystem.getLocal(conf);
			localOutputDir = conf.get("honu.collector." + group +".localOutputDir",
			    "/honu/datasink/");
			if (!localOutputDir.endsWith("/")) {
				localOutputDir += "/";
			}
			Path pLocalOutputDir = new Path(localOutputDir);
			if (!fs.exists(pLocalOutputDir)) {
				boolean exist = fs.mkdirs(pLocalOutputDir);
				if (!exist) {
					throw new WriterException("Cannot create local dataSink dir: "
							+ localOutputDir);
				}
			} else {
				FileStatus fsLocalOutputDir = fs.getFileStatus(pLocalOutputDir);
				if (!fsLocalOutputDir.isDir()) {
					throw new WriterException("local dataSink dir is not a directory: "
							+ localOutputDir);
				}
			}
		} catch (Throwable e) {
			log.fatal("Cannot initialize LocalWriter", e);
			DaemonWatcher.bailout(-1);
		}
		
		String codecClass = null;
		try {
			codecClass = conf.get("honu.collector." + group +".datasink.codec");
	    if (codecClass != null) {
	    		Class classDefinition = Class.forName(codecClass);
	      	codec =  (CompressionCodec) ReflectionUtils.newInstance(classDefinition, conf);
	      	log.info(group +"- Codec:" + codec.getDefaultExtension());
	    }
		}catch(Exception e) {
			log.fatal(group +"- Compression codec for " + codecClass +
	                                           " was not found.", e);
			DaemonWatcher.bailout(-1);
		}
		minPercentFreeDisk = conf.getInt("honu.collector." + group +".minPercentFreeDisk",20);

		rotateInterval = conf.getInt("honu.collector." + group +".rotateInterval",
				1000 * 60 * 5);// defaults to 5 minutes

		initWriteChunkRetries = conf.getInt("honu.collector." + group +".writeChunkRetries", 10);
		writeChunkRetries = initWriteChunkRetries;

		log.info( group + "- rotateInterval is " + rotateInterval);
		log.info( group + "- outputDir is " + localOutputDir);
		log.info( group + "- localFileSystem is " + fs.getUri().toString());
		log.info( group + "- minPercentFreeDisk is " + minPercentFreeDisk);

		statTimer = new Timer();
		statTimer.schedule(new StatReportingTask(), 1000,
				STAT_INTERVAL_SECONDS * 1000);

		fileQueue = new LinkedBlockingQueue<String>();
		localToRemoteHdfsMover = new LocalToRemoteHdfsMover(group,fileQueue, conf);

		this.start();


	}

	private class StatReportingTask extends TimerTask {
		private long lastTs = System.currentTimeMillis();

		public void run() {

			long time = System.currentTimeMillis();
			long currentDs = dataSize;
			dataSize = 0;

			long interval = time - lastTs;
			lastTs = time;

			long dataRate = 1000 * currentDs / interval; // kb/sec
			log.info(group +"- stat:datacollection.writer.local.LocalWriter dataSize="
					+ currentDs + " dataRate=" + dataRate);
		}
	};

	protected void computeTimePeriod() {
		calendar.setTimeInMillis(System.currentTimeMillis());
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		timePeriod = calendar.getTimeInMillis();
		calendar.add(Calendar.HOUR, 1);
		nextTimePeriodComputation = calendar.getTimeInMillis();
	}

	public void close() {
		this.isRunning = false;
		timeOut = System.currentTimeMillis() + (15*1000);
		log.info(group +"- Shutdown request accepted...");
		do {
			try {Thread.sleep(500);} catch(Exception ignored) {}
		}while(!isStopped && (System.currentTimeMillis() < timeOut));
		log.info(group +"- Shutdown request done...");
	}
	
	protected boolean checkForRotate() {
		long now = System.currentTimeMillis();
		if (now >= nextRotate) {
			return true;
		}
		return false;
	}
	public void run() {
		List<Chunk> chunks = new LinkedList<Chunk>();
		Chunk chunk = null;
		long now = 0;
		while ( (isRunning || !chunks.isEmpty()) && (System.currentTimeMillis() < timeOut)) {
			try {
				now = System.currentTimeMillis();
				// Don't rotate if we are not running
				if ( isRunning && (now >= nextRotate ) ){
					rotate();
				}
				if (System.currentTimeMillis() >= nextTimePeriodComputation) {
					computeTimePeriod();
				}
					
				if (chunks.isEmpty()) {
					chunk = this.ChunkQueue.poll(1000, TimeUnit.MILLISECONDS);
					
					if (chunk == null) {
						continue;
					}

					chunks.add(chunk);
					this.ChunkQueue.drainTo(chunks,10);
				}

				add(chunks);
				chunks.clear();
			} catch(InterruptedException e) {
				isRunning = false;
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		log.info(group +"- Shutdown request exit loop ..., ChunkQueue.size at exit time: " + this.ChunkQueue.size());
		try {
			this.internalClose();
			log.info(group +"- Shutdown request internalClose done ...");
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			isStopped = true;
		}
		
	}

	/**
	 *  Best effort, there's no guarantee that chunks 
	 *  have really been written to disk
	 */
	public CommitStatus add(List<Chunk> chunks) throws WriterException {
		Tracer t = Tracer.startNewTracer("honu.server." + group +".addToList"); 
		long now = System.currentTimeMillis();
		if (chunks != null) {
			try {
				chunksWrittenThisRotate = true;
				ChukwaArchiveKey archiveKey = new ChukwaArchiveKey();

				for (Chunk chunk : chunks) {
					archiveKey.setTimePartition(timePeriod);
					archiveKey.setDataType(chunk.getDataType());
					archiveKey.setStreamName(chunk.getTags() + "/" + chunk.getSource()
							+ "/" + chunk.getStreamName());
					archiveKey.setSeqId(chunk.getSeqID());

					if (chunk != null) {
						seqFileWriter.append(archiveKey, chunk);
						// compute size for stats
						dataSize += chunk.getData().length;
					}
				}
				
				long end = System.currentTimeMillis();
				if (log.isDebugEnabled()) {
					log.debug(group+"- duration=" + (end-now) + " size=" + chunks.size());
				}

			} catch (IOException e) {
	      if (t!= null) {
	        t.stopAndLogTracer(); 
	      }
				
				writeChunkRetries--;
				log.error(group +"- Could not save the chunk. ", e);

				if (writeChunkRetries < 0) {
					log
					.fatal(group +"- Too many IOException when trying to write a chunk, Collector is going to exit!");
					DaemonWatcher.bailout(-1);
				}
				throw new WriterException(e);
			}
		}
	
    if (t!= null) {
      t.stopAndLogTracer(); 
    }
		 
		return COMMIT_OK;
	}

	protected void rotate() {
		Tracer t = Tracer.startNewTracer("honu.server." + group +".rotateDataSink"); 
		isRunning = true;
		calendar.setTimeInMillis(System.currentTimeMillis());
		log.info(group +"- start Date [" + calendar.getTime() + "]");
		log.info(group +"- Rotate from " + Thread.currentThread().getName());

		String newName = day.format(calendar.getTime());
		newName += localHostAddr + new java.rmi.server.UID().toString();
		newName = newName.replace("-", "");
		newName = newName.replace(":", "");
		// newName = newName.replace(".", "");
		newName = localOutputDir + "/" + newName.trim();
		
		try {
			FSDataOutputStream previousOutputStr = currentOutputStr;
			Path previousPath = currentPath;
			String previousFileName = currentFileName;

			if (previousOutputStr != null) {
			  seqFileWriter.close();
				previousOutputStr.close();
				if (chunksWrittenThisRotate) {
					fs.rename(previousPath, new Path(previousFileName + ".done"));
					fileQueue.add(previousFileName + ".done");
				} else {
					log.info(group+"- no chunks written to " + previousPath + ", deleting");
					fs.delete(previousPath, false);
				}
			}
			
			Path newOutputPath = new Path(newName + ".chukwa");
			FSDataOutputStream newOutputStr = fs.create(newOutputPath);

			currentOutputStr = newOutputStr;
			currentPath = newOutputPath;
			currentFileName = newName;
			chunksWrittenThisRotate = false;
			
			if (codec != null) {
				seqFileWriter = SequenceFile.createWriter(conf, newOutputStr,
						ChukwaArchiveKey.class, ChunkImpl.class,
						SequenceFile.CompressionType.BLOCK, codec);
			} else {
				seqFileWriter = SequenceFile.createWriter(conf, newOutputStr,
						ChukwaArchiveKey.class, ChunkImpl.class,
						SequenceFile.CompressionType.NONE, codec);
			}
			
		} catch (Throwable e) {
      if (t!= null) {
        t.stopAndLogTracer(); 
      }
			
			log.fatal(group+"- Throwable Exception in rotate. Exiting!", e);
			// Shutting down the collector
			// Watchdog will re-start it automatically
			DaemonWatcher.bailout(-1);
		}

		// Check for disk space
		File directory4Space = new File(localOutputDir);
		long totalSpace = directory4Space.getTotalSpace();
		long freeSpace = directory4Space.getFreeSpace();
		long minFreeAvailable = (totalSpace * minPercentFreeDisk) /100;

		if (log.isDebugEnabled()) {
			log.debug( group +"- Directory: " + localOutputDir + ", totalSpace: " + totalSpace 
					+ ", freeSpace: " + freeSpace + ", minFreeAvailable: " + minFreeAvailable
					+ ", percentFreeDisk: " + minPercentFreeDisk);
		}

		if (freeSpace < minFreeAvailable) {
			log.fatal( group +"- No space left on device, Bail out!");
			DaemonWatcher.bailout(-1);
		} 
		nextRotate = System.currentTimeMillis() + rotateInterval;
		
    if (t!= null) {
      t.stopAndLogTracer(); 
    }
	}

	protected void internalClose() {
	  try {
	    if (seqFileWriter != null) {
	      seqFileWriter.close();
	    }

	    if (this.currentOutputStr != null) {
	      this.currentOutputStr.close();

	    }
	    if (localToRemoteHdfsMover != null) {
	      localToRemoteHdfsMover.shutdown();
	    }

	    fs.rename(currentPath, new Path(currentFileName + ".done"));
	    log.info( group +" close on " + currentFileName + ".done done");
	  } catch (IOException e) {
	    log.error( group + "- failed to close and rename stream", e);
	  }

	  if (rotateTimer != null) {
	    rotateTimer.cancel();
	  }

	  if (statTimer != null) {
	    statTimer.cancel();
	  }

	}
}
