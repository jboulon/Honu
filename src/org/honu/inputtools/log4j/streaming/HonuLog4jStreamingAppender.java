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
package org.honu.inputtools.log4j.streaming;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.honu.inputtools.streaming.MessageConsumer;
import org.honu.inputtools.streaming.MessageManager;
import org.honu.util.ISO8601;
import org.honu.util.KeyValueSerialization;


public class HonuLog4jStreamingAppender  extends AppenderSkeleton{

  protected static final Object lock = new Object();
	protected static MessageManager manager = null;
	protected static char fieldDelim = KeyValueSerialization.fieldDelim;
	protected static char fieldEqual = KeyValueSerialization.fieldEqual;
	
	protected String source = "N/A";
	protected String streamName = "N/A";
	protected String application = "N/A";
	protected String dataType = "default";
	protected String tags = " cluster=\"default\" ";
	protected int messageQueueThreshold = 300;
	
	protected String senderType = null;
	protected String collectors = "";
	protected long samplingPeriod = 500;
	protected int maxMessageCountPerChunk = 300;
	protected long senderTimeOut =  20000;
	protected int coreThreadCount = (int)(1.8*Runtime.getRuntime().availableProcessors());
	protected MessageConsumer consumer = null;
	protected ThreadLocalStringBuilder stringBuilder = new ThreadLocalStringBuilder();
	protected volatile boolean initDone = false;
	
	protected static String localHostAddr = null;
	static {
    try {
      localHostAddr = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      localHostAddr = "N/A";
    }
  }
	
  @SuppressWarnings("unchecked")
  protected static class ThreadLocalStringBuilder extends ThreadLocal {
    public Object initialValue() {
      return new StringBuilder();
    }
   
    public StringBuilder getSB() { 
      return (StringBuilder) super.get(); 
    }
  }
	

	
	public HonuLog4jStreamingAppender() {
		source = localHostAddr;
	}
	
	
	@Override
	public void activateOptions() {
		super.activateOptions();

		if (manager == null) {
			manager = MessageManager.getInstance();
			Properties senderProperties = new Properties();
			senderProperties.put(MessageManager.collectorsProperty, collectors);
			senderProperties.put(MessageManager.senderTimeOutProperty, ""+senderTimeOut);
			senderProperties.put(MessageManager.coreThreadProperty, ""+coreThreadCount);
			if (senderType != null) {
				senderProperties.put(MessageManager.senderTypeProperty, senderType);
			}
			manager.start(senderProperties);
		}

		if (initDone == false) {
			synchronized (lock) {
			  if (initDone == false) {
			    initDone = true;
			  } else {
			    return;
			  }
			}
    
			Properties consumerProperties = new Properties();
			consumerProperties.put(MessageManager.sourceProperty, source);
			consumerProperties.put(MessageManager.streamNameProperty, streamName);
			consumerProperties.put(MessageManager.applicationProperty, application);
			consumerProperties.put(MessageManager.dataTypeProperty, dataType);

			tags = tags + " version=\"" 
			+ HonuLog4jStreamingAppender.class.getPackage().getImplementationVersion() + "\" ";

			consumerProperties.put(MessageManager.tagsProperty, tags);
			consumerProperties.put(MessageManager.thresholdProperty,""+ messageQueueThreshold);
			consumerProperties.put(MessageManager.samplingPeriodProperty, ""
			    + samplingPeriod);
			consumerProperties.put(MessageManager.maxMessageCountPerChunkProperty, ""
			    + maxMessageCountPerChunk);
			consumer = manager.createConsumer(consumerProperties);

		}
  }

	
	@SuppressWarnings("unchecked")
  @Override
	protected void append(LoggingEvent event) {

		StringBuilder sb = this.stringBuilder.getSB();
		sb.setLength(0);

		ISO8601.format(new java.util.Date(),sb).append(fieldDelim);
		sb.append(event.getLevel()).append(fieldDelim).append(event.getLoggerName());
		
		Object obj = event.getMessage();

		// time UTC^]Level^]Map
		if (obj instanceof Map) {
			Map map = (Map) event.getMessage();
			Iterator it = map.keySet().iterator();
			Object key = null;
			while(it.hasNext()){
				key = it.next();
				sb.append(fieldDelim).append(key.toString()).append(fieldEqual).append(map.get(key));
			}
		} else { 
			// time UTC^]Level^]String
			sb.append(fieldDelim).append(obj.toString());
		}


		// Extract exceptions
		String[] s = event.getThrowableStrRep();
		if (s != null) {
			sb.append(fieldDelim).append("Exception").append(fieldEqual);
			int len = s.length;
			for(int i = 0; i < len; i++) {
				sb.append(s[i]).append("\n");
			}
		}
		consumer.process(sb.toString());
	}
	
	@Override
  public void close() {
		MessageManager.getInstance().shutdown();
  }

	@Override
  public boolean requiresLayout() {
		return false;
  }

	public String getSource() {
  	return source;
  }

	public void setSource(String source) {
  	this.source = source;
  }

	public String getStreamName() {
  	return streamName;
  }

	public void setStreamName(String streamName) {
  	this.streamName = streamName;
  }

	public String getApplication() {
  	return application;
  }

	public void setApplication(String application) {
  	this.application = application;
  }

	public String getDataType() {
  	return dataType;
  }

	public void setDataType(String dataType) {
  	this.dataType = dataType;
  }

	public String getTags() {
  	return tags;
  }

	public void setTags(String tags) {
  	this.tags = tags;
  }

	public long getSamplingPeriod() {
  	return samplingPeriod;
  }

	public void setSamplingPeriod(long samplingPeriod) {
  	this.samplingPeriod = samplingPeriod;
  }

	public int getMaxMessageCountPerChunk() {
  	return maxMessageCountPerChunk;
  }

	public void setMaxMessageCountPerChunk(int maxMessageCountPerChunk) {
  	this.maxMessageCountPerChunk = maxMessageCountPerChunk;
  }

	public long getSenderTimeOut() {
  	return senderTimeOut;
  }

	public void setSenderTimeOut(long senderTimeOut) {
  	this.senderTimeOut = senderTimeOut;
  }

	public String getCollectors() {
  	return collectors;
  }

	public void setCollectors(String collectors) {
  	this.collectors = collectors;
  }

	public int getCoreThreadCount() {
  	return coreThreadCount;
  }

	public void setCoreThreadCount(int coreThreadCount) {
  	this.coreThreadCount = coreThreadCount;
  }

	public String getSenderType() {
  	return senderType;
  }

	public void setSenderType(String senderType) {
  	this.senderType = senderType;
  }

	public int getMessageQueueThreshold() {
  	return messageQueueThreshold;
  }

	public void setMessageQueueThreshold(int messageQueueThreshold) {
  	this.messageQueueThreshold = messageQueueThreshold;
  }

}
