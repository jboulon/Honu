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
package org.honu.inputtools.streaming;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.honu.thrift.HonuCollector;
import org.honu.thrift.Result;
import org.honu.thrift.ResultCode;
import org.honu.thrift.TChunk;
import org.honu.util.KeyValueSerialization;
import org.apache.log4j.Logger;
import org.honu.util.Counter;
import org.honu.util.Tracer;

public abstract class MessageSender implements Runnable {
  static Logger log = Logger.getLogger(MessageSender.class);
  static Logger logApp = Logger.getLogger("honu.sender");

  protected int DEFAULT_POLL_TIMEOUT = 500;
  protected int MAX_POLL_TIMEOUT = 10000;
  protected int SENDER_TIMEOUT = 5000;
  protected int MIN_LEASE_TIME = 10;
  
  protected long SHUTDOWN_TIME = 0;

  

  protected HonuCollector.Client collector = null;
  protected CollectorInfo currentCollectorInfo = null;
  protected BlockingQueue<TChunk> chunkQueue = null;
  protected MessageManager messageManager = null;

  protected volatile boolean running = true;
  protected Random random = new Random();
  protected long leaseTs = 0;
  
  private KeyValueSerialization kv = new KeyValueSerialization();

  
  
  long statFrequency =  60 * 1000;
  long nextStatPeriod = 0;

  int lineCount = 0;
  int chunkCount = 0;
  int exceptionCount = 0;

  public MessageSender(MessageManager messageManager, boolean coreThread,
      int queueThreshold, int senderTimeOut,
      BlockingQueue<TChunk> chunkQueue) {

    this.SENDER_TIMEOUT = senderTimeOut;
    this.chunkQueue = chunkQueue;
    this.messageManager = messageManager;

  }

  /**
   * Close current connection and make everything ready 
   * for openConnection to be called
   * @throws CommunicationException
   */
  protected abstract void closeConnection()  throws CommunicationException;
  
  /** A valid connection should be return, if not possible then throws a 
   * CommunicationException.
   * @throws CommunicationException
   */
  protected abstract void openConnection() throws CommunicationException;
  
  protected void initConnection() throws CommunicationException{
    openConnection();
    // get lease for minLeaseTime +/- 5 minutes
    int minutes = MIN_LEASE_TIME + random.nextInt(5);
    leaseTs = System.currentTimeMillis()+ (minutes*60*1000);
    logApp.info("Lease valid for " + minutes 
        + " mins - Current lease will expire on " + new java.util.Date(leaseTs)
        + " - talking to collector: " + this.currentCollectorInfo);
  }
  
  public void shutdown() {
    this.running = false;
    SHUTDOWN_TIME = System.currentTimeMillis() + SENDER_TIMEOUT;
    logApp.info("[==HONU==] Honu message sender [" + Thread.currentThread().getId()+ "] shutdown in progress, messageQueue:" + chunkQueue.size());
  }

  protected boolean shutDownNow() {
    return (!running && (System.currentTimeMillis() > SHUTDOWN_TIME));
  }


  public void run() {
    TChunk chunk = null;

    long timeOut = DEFAULT_POLL_TIMEOUT;
    long curr = 0l;

    while (running || (chunkQueue.size() != 0)) {
      try {
        curr = System.currentTimeMillis();

        if (shutDownNow()) {
          logApp.info("[==HONU==] Honu message sender [" + Thread.currentThread().getId()+ "] ShutdownNow");
          break;
        }

        if (curr >= nextStatPeriod ) {
          kv.startMessage("HonuSenderStats");
          kv.addKeyValue("chunkCount", chunkCount);
          kv.addKeyValue("lineCount", lineCount);
          kv.addKeyValue("exceptionCount", exceptionCount);
          kv.addKeyValue("period", statFrequency);
          log.info(kv.generateMessage());

          // Keep System.out for debug purpose
          if (log.isDebugEnabled()) {
            System.out.println(Thread.currentThread().getId() + " - "
                + new java.util.Date() + " - Chunk sent:" + chunkCount
                + " - lines:" + lineCount + " - in: 1 min+" + (curr - nextStatPeriod)
                + " ms");	
          }

          lineCount = 0;
          chunkCount = 0;
          exceptionCount = 0;
          nextStatPeriod = System.currentTimeMillis() + statFrequency;
        }

        chunk = chunkQueue.poll(timeOut, TimeUnit.MILLISECONDS);

        // If there's nothing to work on
        // increment sleep time up to maxPollTimeOut
        if (chunk == null) {
          if (timeOut < MAX_POLL_TIMEOUT) {
            timeOut += DEFAULT_POLL_TIMEOUT;
          }
          continue;
        }

        timeOut = DEFAULT_POLL_TIMEOUT;
        Tracer t = Tracer.startNewTracer("honu.client.sendChunk"); 
        try {
          (new Tracer("honu.client.chunkQueueSize [chunks, not msec]", chunkQueue.size())).logTracer(); 
          (new Tracer("honu.client." + chunk.getApplication()  + ".chunkQueueSize [chunks, not msec]", chunkQueue.size())).logTracer(); 
        }catch(Exception ignored) {

        }
        
        if (System.currentTimeMillis() > leaseTs) {
          logApp.info("Time for lease renewal");
          closeConnection();
        }
        
        sendChunk(chunk);
        if (t!= null) {
          t.stopAndLogTracer(); 
        }
        chunkCount++;
        lineCount += chunk.getLogEventsSize();

        Thread.yield();
        
      }  catch (Throwable e) { 
        logApp.warn("Error in main loop",e );
      }
    }
    
    if (messageManager != null){
      messageManager.senderShutdownCallback();
    }

    logApp.info("[==HONU==] Honu message sender [" + Thread.currentThread().getId()+ "] shutdown completed, messageQueue:" + chunkQueue.size());
  }

  protected int randomSleep(int duration, boolean isSecond) {
    int sleep = random.nextInt(duration);
    if (isSecond) {
      sleep = sleep * 1000;
    }
    logApp.info("[==HONU==] Honu message sender [" + Thread.currentThread().getId()+ "] going to sleep, messageQueue:" + chunkQueue.size());
    try {Thread.sleep(sleep);} catch (Exception ignored) {}

    return sleep;
  }

  protected void sendChunk(TChunk chunk) {
    boolean firstTryFailed = false;
    boolean hasBeenSent = false;

    int errorCount = 0;
    int tryLaterErrorCount = 0;

    Result result = null;


    do {
      try {
        Tracer t = Tracer.startNewTracer("honu.client.processChunk");
        try {

          if ((collector == null) || (tryLaterErrorCount >= 3)) {
            tryLaterErrorCount = 0;
            initConnection();
          }

          result = collector.process(chunk);
          if (t!= null){
            t.stopAndLogTracer(); 
          }
         

          if (result.getResultCode() == ResultCode.TRY_LATER) {
            Counter.increment("honu.client.tryLater"); 
            Counter.increment("honu.client."+ chunk.getApplication() +".tryLater"); 

            tryLaterErrorCount++;

            // If there's more than 3 TRY_LATER
            // on a collector the switch
            if (tryLaterErrorCount > 3) {
              collector = null;
              errorCount++;
            } else {
              randomSleep(5 * tryLaterErrorCount, true);
            }

          } else {
            Counter.increment("honu.client.chunkCount"); 
            Counter.increment("honu.client.logCount",chunk.getLogEventsSize());

            Counter.increment("honu.client."+ chunk.getApplication() +".chunkCount"); 
            Counter.increment("honu.client."+ chunk.getApplication() +".logCount",chunk.getLogEventsSize());
          }
        } catch (Throwable e) {

          try {
            log.warn("exception in sendChunk", e);
            if (t != null) {
              t.stopAndLogTracer(); 
            }

            Counter.increment("honu.client.exceptionCount"); 

            exceptionCount++;
            collector = null;
            errorCount++;
            randomSleep(300, false);

          }catch (Throwable eIgnored) { /* Ignore */}

        }
        if (errorCount >= CollectorRegistry.getInstance().getCollectorCount()) {
          if (firstTryFailed == true) {
            randomSleep(30, true);
            collector = null;
            errorCount = 0;
          } else {
            firstTryFailed = true;
            randomSleep(15, true);
            collector = null;
            errorCount = 0;
          }
        }
        if (result != null && result.getResultCode() == ResultCode.OK && result.isSetMessage()) {
          hasBeenSent = true;
        }

        if (hasBeenSent == false && shutDownNow()) {
          System.err.println("Need to shutdown now --" + Thread.currentThread().getName() + " -- Data has not been sent over:" + chunk.toString());
          hasBeenSent = true; 
        }
        
      }catch (Throwable e2) {
        logApp.warn("Error in sendChunk",e2 );
      }

    }while (!hasBeenSent);

  }

}
