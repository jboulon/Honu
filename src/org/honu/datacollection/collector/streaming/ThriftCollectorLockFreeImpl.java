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
package org.honu.datacollection.collector.streaming;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkBuilder;
import org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter;
import org.honu.thrift.HonuCollector;
import org.honu.thrift.Result;
import org.honu.thrift.ResultCode;
import org.honu.thrift.TChunk;
import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.honu.configuration.HonuConfigurationFactory;
import org.honu.datacollection.writer.localfs.LockFreeWriter;
import org.honu.services.HonuServiceImpl;
import org.honu.util.Counter;
import org.honu.util.Tracer;
import org.mortbay.log.Log;


public class ThriftCollectorLockFreeImpl extends HonuServiceImpl implements HonuCollector.Iface{

	private static final String chunkCountField = "chunkCount";
	static final Object lock = new Object();
  
	static long msgCount = 0l;
	
	protected BlockingQueue<Chunk> chunkQueue = new ArrayBlockingQueue<Chunk>(50);	
	protected ChukwaWriter writer = null;
	
	volatile boolean isDebug = false;
	volatile boolean isRunning = false;
	
	class ClientFinalizer extends Thread {
		private ChukwaWriter writer = null;
		public ClientFinalizer(ChukwaWriter writer) {
			this.writer = writer;
		}

		public void run() {
			try {
				writer.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	

	
	public ThriftCollectorLockFreeImpl() throws Exception {
		this.counters.put(chunkCountField, new AtomicLong(0L));
		Configuration confLocalWriter = HonuConfigurationFactory.getInstance().getCollectorConfiguration();
		// Are we running in debug mode
		String honuMode = System.getenv("HONU_MODE");
		if (honuMode != null && honuMode.equalsIgnoreCase("debug")) {
		  isDebug = true;
		}
		
    synchronized(lock) {
    	if (writer == null) {
    		 writer = new LockFreeWriter("defaultGroup",chunkQueue);
    	   writer.init(confLocalWriter);
    	   
    	   ClientFinalizer clientFinalizer = new ClientFinalizer(writer);
    	   Runtime.getRuntime().addShutdownHook(clientFinalizer);
    	   
         isRunning = true;
         
    	}
    }
	}
		
	@Override
  public String getName() throws TException {
	  return "HonuThriftCollector";
  }

	@Override
  public String getVersion() throws TException {
	  return "V1.1.1-LockFree";
  }
  public Result process(TChunk tChunk) throws TException {
    
    // Stop adding chunks if it's no running
    if (!isRunning) {
      Log.warn("Rejecting some incoming trafic!");
      Result result = new Result();
      result.setMessage("Shutting down");
      result.setResultCode(ResultCode.TRY_LATER);
      return result;
    }
    
    // If there's no log Events then return OK
    if (tChunk.getLogEventsSize() == 0) {
      Result result = new Result();
      result.setMessage(""+tChunk.getSeqId());
      result.setResultCode(ResultCode.OK);
      return result;
    }
    
  	 Tracer t = Tracer.startNewTracer("honu.server.processChunk"); 
		//this.counters.get(chunkCountField).incrementAndGet();
		
		ChunkBuilder cb = new ChunkBuilder();
		List<String> logEvents = tChunk.getLogEvents();
		
		
		for(String logEvent :logEvents) {
			cb.addRecord(logEvent.getBytes());
		}
		
		Chunk c = cb.getChunk();
		c.setApplication(tChunk.getApplication());
		c.setDataType(tChunk.getDataType());
		c.setSeqID(tChunk.getSeqId());
		c.setSource(tChunk.getSource());
	  c.setTags(tChunk.getTags());
	  
		if (isDebug) {
		  System.out.println("\n\t ===============");
		  System.out.println("tChunk.getApplication() :" + tChunk.getApplication());
	    System.out.println("tChunk.getDataType() :" + tChunk.getDataType());
	    System.out.println("tChunk.getSeqId() :" + tChunk.getSeqId());
	    System.out.println("tChunk.getSource() :" + tChunk.getSource());
	    System.out.println("tChunk.getStreamName() :" + tChunk.getStreamName());
	    System.out.println("tChunk.getTags() :" + tChunk.getTags());
	    
	    
	    System.out.println("c.getApplication() :" + c.getApplication());
	    System.out.println("c.getDataType() :" + c.getDataType());
	    System.out.println("c.getSeqID() :" + c.getSeqID());
	    System.out.println("c.getSource() :" + c.getSource());
	    System.out.println("c.getTags() :" + c.getTags());
	    System.out.println("c.getData()" + new String( c.getData()));
	    
		}
	
		
		
	  boolean addResult = false;
	  
	  try {
	      addResult = chunkQueue.offer(c, 2000, TimeUnit.MILLISECONDS);
    }catch (OutOfMemoryError ex) {
    	ex.printStackTrace();
    	DaemonWatcher.bailout(-1);
    }
	  catch (Throwable e) {
	    e.printStackTrace();
	    addResult = false;
    }
	
	  Result result = new Result();
	  
	  if (addResult) {
	    try {
	      Counter.increment("honu.server.chunkCount"); 
	      Counter.increment("honu.server.logCount",logEvents.size());
	      
	      Counter.increment("honu.server."+ tChunk.getApplication() +".chunkCount"); 
	      Counter.increment("honu.server."+ tChunk.getApplication() +".logCount",logEvents.size());  
	     
        (new Tracer("honu.server.chunkSize [messages, not msec]", logEvents.size())).logTracer(); 
        (new Tracer("honu.server." + tChunk.getApplication()  + ".chunkSize [messages, not msec]", logEvents.size())).logTracer(); 
	    }catch(Exception ignored) {
	      
	    }
	  	
		
	  	result.setMessage(""+tChunk.getSeqId());
		  result.setResultCode(ResultCode.OK);
	  } else {
	    try {
	      Counter.increment("honu.server.tryLater"); 
	      Counter.increment("honu.server."+ tChunk.getApplication() +".tryLater");   
	    }catch(Exception ignored) {
	      
	    }
	  	
	  	result.setMessage(""+tChunk.getSeqId());
		  result.setResultCode(ResultCode.TRY_LATER);
	  }
	  if (t!= null) {
	    t.stopAndLogTracer();
	  }
	  
	  return result;
  }

  public long shutdown() throws TException {
    // Prevent shutdown now since there's no security
    // Use Ctrl-C on the collector to gracefully shutdown
    
    /*
    isRunning = false;
    ClientFinalizer clientFinalizer = new ClientFinalizer(writer);
    clientFinalizer.start();
    return 0;
    */
    return -1;
  }


}
