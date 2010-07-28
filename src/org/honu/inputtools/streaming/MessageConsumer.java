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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.honu.thrift.TChunk;
import org.honu.util.KeyValueSerialization;
import org.apache.log4j.Logger;
import org.honu.util.Counter;
import org.honu.util.Tracer;

public class MessageConsumer implements Runnable {
  static Logger log = Logger.getLogger(MessageConsumer.class);
  static Logger logApp = Logger.getLogger("honu.consumer");
  
  private BlockingQueue<TChunk> chunkQueue = null;
  private List<String> messages = new LinkedList<String>();

  private Object wakeUpLink = new Object();

  private String source = null;
  private String streamname = null;
  private String application = null;
  private String dataType = null;
  private String tags = null;

  private int threshold = -1;
  private int defaultThreshold = 0;
  private long samplingPeriod = -1;
  private int maxMessageCountPerChunk = 300;
  private long defaultPollTimeOut = 500;
  private int maxPollTimeOut = 10000;
  private int maxChunkQueueSize = 10000;

  private volatile boolean running = true;
  private volatile boolean isDone = false;
  
  private int statFrequency = 60 * 1000;
  private int maxQueueSize = 0;
  private int messageCount = 0;
  private long nextStatPeriod = 0;
  private KeyValueSerialization kv = new KeyValueSerialization();

  public MessageConsumer(String source, String streamname, String application,
      String dataType, String tags, int threshold, long samplingPeriod,
      int maxMessageCountPerChunk, BlockingQueue<TChunk> chunkQueue,
      int maxChunkQueueSize) {

    this.source = source;
    this.streamname = streamname;
    this.application = application;
    this.dataType = dataType;
    this.tags = tags;

    this.chunkQueue = chunkQueue;
    this.samplingPeriod = samplingPeriod;
    this.defaultPollTimeOut = samplingPeriod;
    this.threshold = threshold;
    this.defaultThreshold = threshold;
    this.maxMessageCountPerChunk = maxMessageCountPerChunk;
    this.maxChunkQueueSize = maxChunkQueueSize;
  }

  public void run() {
    if (log.isDebugEnabled()) {
      log.debug("start running [" + this.dataType + "]");
    }

    while (running) {
      try {

        // OutputStats:
        if (System.currentTimeMillis() >= nextStatPeriod) {
          kv.startMessage("HonuMessageConsumerStats");
          kv.addKeyValue("msgCount", messageCount);
          messageCount = 0;
          kv.addKeyValue("maxQueueSize", maxQueueSize);
          maxQueueSize = 0;
          log.info(kv.generateMessage());
          nextStatPeriod = System.currentTimeMillis() + statFrequency;
        }

        if (!messages.isEmpty()) {
          produceChunk();
          samplingPeriod = defaultPollTimeOut;
          threshold = defaultThreshold;
        } else if (samplingPeriod < maxPollTimeOut) {
          samplingPeriod += defaultPollTimeOut;
        }

        if ( (messages.size() < threshold) && (running == true)){
          synchronized (wakeUpLink) {
            wakeUpLink.wait(samplingPeriod);
          }
        } else {
          Thread.yield();
        }

      } catch (Exception e) {
        e.printStackTrace();
        try { Thread.sleep(300); }
        catch (Exception esleep) { /* Ignored Exception */}
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("Going to exit- final loop");
    }

    long timeOut = System.currentTimeMillis() + (20 * 1000); // now + 20 secs
    // Flush the queue
    logApp.info("[==HONU==] Honu message consumner [" + this.dataType + "] Shutdown in progress");
    while (!messages.isEmpty() && (System.currentTimeMillis() <= timeOut)) {
      try {
        produceChunk();
      } catch (Exception e) {
        log.error("Exception:", e);
      }
    }
    isDone = true;
    logApp.info("[==HONU==] Honu message consumner [" + this.dataType + "] shutdown completed, messageQueue:" + messages.size());
  }

  public boolean isFinished() {
    return this.isDone;
  }
  private void produceChunk() throws InterruptedException {
    TChunk chunk = new TChunk();
    chunk.setSeqId(System.currentTimeMillis());
    chunk.setSource(source);
    chunk.setStreamName(streamname);
    chunk.setApplication(application);
    chunk.setDataType(dataType);
    chunk.setTags(tags);
    List<String> logEvents = new LinkedList<String>();
    chunk.setLogEvents(logEvents);

    int qSize = messages.size();
    if (qSize > maxQueueSize) {
      maxQueueSize = qSize;
    }

    int count = 0;
    // Need to be the only one to access
    // the messages list
    synchronized (wakeUpLink) {
      do {
        logEvents.add(messages.remove(0));
        count++;
      } while (!messages.isEmpty() && count < maxMessageCountPerChunk);
    }

    try {
      (new Tracer("honu.client.messageQueueSize [messages, not msec]", messages.size())).logTracer(); 
      (new Tracer("honu.client." + chunk.getApplication()  + ".messageQueueSize [messages, not msec]", messages.size())).logTracer(); 
    }catch(Exception ignored) {
      
    }
    // Drop on the floor
    // TODO instead of dropping on the floor could write to
    // a backup file
    if (chunkQueue.size() >= maxChunkQueueSize) {
      try {
        Counter.increment("honu.client.lostChunks");
        Counter.increment("honu.client.lostMessages", chunk
            .getLogEventsSize());

        Counter.increment("honu.client." + chunk.getApplication()
            + ".lostChunks");
        Counter.increment("honu.client." + chunk.getApplication()
            + ".lostMessages", chunk.getLogEventsSize());

      } catch (Exception ignored) {

      }

      kv.startMessage("HonuLostStats");
      kv.addKeyValue("lostChunk", 1);
      kv.addKeyValue("lostLines", chunk.getLogEventsSize());
      kv.addKeyValue("RecordType", chunk.getDataType());
      kv.addKeyValue("SeqId", chunk.getSeqId());
      kv.addKeyValue("period", statFrequency);
      log.error(kv.generateMessage());

      MessageManager.getInstance().updateLostDataStats(chunk);
    } else {
      chunkQueue.put(chunk);
    }
  }

  public void shutdown() {
    this.running = false;
    synchronized (wakeUpLink) {
      wakeUpLink.notifyAll();
    }
  }

  public int getMessageCount() {
    return this.messages.size();
  }

  public void process(String message) {
    // Need to be the only one to access
    // the messages list
    synchronized (wakeUpLink) {
      messageCount++;
      messages.add(message);
      if (messages.size() >= threshold) {
        wakeUpLink.notifyAll();
      }
    }
  }

}
