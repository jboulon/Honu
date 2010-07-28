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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.honu.thrift.TChunk;

public class MessageManager {


  private static final Object lock = new Object();

  public static final String thresholdProperty = "messageQueueThreshold";
  public static final String samplingPeriodProperty = "samplingPeriod";
  public static final String maxMessageCountPerChunkProperty = "maxMessageCountPerChunk";
  public static final String sourceProperty = "source";
  public static final String streamNameProperty = "streamName";
  public static final String applicationProperty = "application";
  public static final String dataTypeProperty = "dataType";
  public static final String tagsProperty = "tags";

  public static final String collectorsProperty = "collectors";
  public static final String senderQueueThresholdProperty = "senderQueueThreshold";
  public static final String senderTimeOutProperty = "senderTimeOut";
  public static final String coreThreadProperty = "coreThreadCount";
  public static final String senderTypeProperty = "senderType";
  public static final String maxChunkCountProperty = "maxChunkCount";


  public static final int TSocketSender =100;
  public static final int HTTPSender = 200;


  protected static MessageManager messageManager = null;

  private List<MessageConsumer> consumers = new LinkedList<MessageConsumer>();
  private List<MessageSender> senders = new LinkedList<MessageSender>();

  private ThreadGroup consumerGroup = null;
  private ThreadGroup senderGroup = null;

  private BlockingQueue<TChunk> chunkQueue = new  LinkedBlockingQueue<TChunk>();
  private List<CollectorInfo> collectors = new LinkedList<CollectorInfo>();

  private volatile boolean started = false;
  private volatile boolean shutdownInProgress = false;

  private int senderTimeOut =  20000;
  private int senderQueueThreshold = 10;
  private int coreThread =  (2*Runtime.getRuntime().availableProcessors());
  private int maxChunkCount = 1000;
  private AtomicInteger coreThreadCount = new AtomicInteger();

  private AtomicLong lostChunkCount = new AtomicLong(0);
  private AtomicLong lostLineCount = new AtomicLong(0);

  class ClientFinalizer extends Thread {
    public void run() {
      try {
        MessageManager.getInstance().shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }




  private MessageManager() {
    consumerGroup = new ThreadGroup("MessageConsumers");
    senderGroup = new ThreadGroup("MessageSenders");
    Runtime.getRuntime().addShutdownHook(new ClientFinalizer());
  }


  public static MessageManager getInstance() {
    synchronized (lock) {
      if (messageManager == null) {
        messageManager = new MessageManager();
      }
    }
    return messageManager;
  }

  public void start(Properties senderProperties) {
    if (started) {
      return;
    }


    String strCollectors = senderProperties.getProperty(collectorsProperty);
    if (strCollectors == null ) {
      throw new RuntimeException("Collectors are empty.");
    }

    try {
      senderTimeOut = Integer.parseInt(senderProperties.getProperty(senderTimeOutProperty, "20000")); 
    } catch(NumberFormatException e) {
      senderTimeOut = 20000;
    }

    try {
      int threadCount = Integer.parseInt(senderProperties.getProperty(coreThreadProperty, ""+coreThread)); 
      coreThread = threadCount;
    } catch(NumberFormatException ignored) {
      // default value
    }

    try {
      int _maxChunkCount = Integer.parseInt(senderProperties.getProperty(maxChunkCountProperty, ""+maxChunkCount));
      maxChunkCount = _maxChunkCount;
    } catch(NumberFormatException ignored) {
      // default value
    }

    int senderType = TSocketSender;
    String str = senderProperties.getProperty(senderTypeProperty); 
    if (str != null && str.equalsIgnoreCase("http")) {
      senderType = HTTPSender;
    }
    
    synchronized (lock) {
      if (!started) {
        started = true;
        shutdownInProgress = false;
      } else {
        return;
      }
    }
    
    CollectorRegistry.getInstance().setCollectors(strCollectors);
    
    // pre-start coreThread
    for (int i=0;i<coreThread;i++) {
      startNewSender(true,senderType);
    }   
  }

  public void senderShutdownCallback() {
    coreThreadCount.decrementAndGet();
  }

  protected void startNewSender(boolean isCoreThread, int senderclass) {
    coreThreadCount.incrementAndGet();
    Collections.shuffle(collectors);
    MessageSender sender = null;
    // Will be better to use a factory but it's ok for now
    if (senderclass == MessageManager.TSocketSender) {
      sender = new TSocketMessageSender( this,isCoreThread,senderQueueThreshold,senderTimeOut, chunkQueue);
    } else {
      sender = new HTTPMessageSender( this,isCoreThread,senderQueueThreshold,senderTimeOut, chunkQueue);
    }
    senders.add(sender);
    Thread t = new Thread(senderGroup,sender);
    t.setDaemon(false);
    t.start();
  }


  public MessageConsumer createConsumer(Properties properties) {
    int threshold = 0;
    long samplingPeriod = 0;
    int maxMessageCountPerChunk = 0;
    try {
      threshold = Integer.parseInt(properties.getProperty(thresholdProperty, "50"));
    }catch(NumberFormatException e) {
      threshold = 50;
    }
    try {
      samplingPeriod = Long.parseLong(properties.getProperty(samplingPeriodProperty, "10000")); // 10 secs
    }catch(NumberFormatException e) {
      samplingPeriod = 10000;
    }

    try {
      maxMessageCountPerChunk = Integer.parseInt(properties.getProperty(maxMessageCountPerChunkProperty, "100"));
    }catch(NumberFormatException e) {
      maxMessageCountPerChunk = 100;
    }

    MessageConsumer consumer = new MessageConsumer(	properties.getProperty(sourceProperty, "N/A"),properties.getProperty(streamNameProperty, "N/A"),
        properties.getProperty(applicationProperty, "N/A"),properties.getProperty(dataTypeProperty, "N/A"),
        properties.getProperty(tagsProperty, "N/A"),
        threshold, samplingPeriod,maxMessageCountPerChunk,chunkQueue,maxChunkCount);
    Thread consumerThread = new Thread(consumerGroup,consumer);
    consumerThread.start();
    consumers.add(consumer);
    return consumer;
  }

  public void shutdown() {
    synchronized(lock) {
      if (this.shutdownInProgress == true) {
        return;
      }
      this.shutdownInProgress = true;
    }

    for(MessageConsumer consumer: consumers) {
      consumer.shutdown();
    }

    // Flush all input queues
    long flushTimeOut = System.currentTimeMillis() + 10*1000;
    for(MessageConsumer consumer: consumers) {
      while (consumer.isFinished() == true) {
        if (System.currentTimeMillis() >= flushTimeOut) {
          break;
        }
        try {Thread.sleep(100);}
        catch(Exception e) { /*ignored exception */}
      }
    }

    for(MessageSender sender: senders) {
      sender.shutdown();
    }
    
    started = false;
  }

  public void updateLostDataStats(TChunk chunk) {
    this.lostLineCount.addAndGet(chunk.getLogEventsSize());
    this.lostChunkCount.getAndIncrement();
  }

  public boolean isAlive() {
    return (consumerGroup.activeCount() != 0 || senderGroup.activeCount() != 0);
  }

  public BlockingQueue<TChunk> getQueue() {
    return chunkQueue;
  }

}
