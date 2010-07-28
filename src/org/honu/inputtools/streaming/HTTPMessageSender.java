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

import java.util.concurrent.BlockingQueue;

import org.honu.thrift.HonuCollector;
import org.honu.thrift.ServiceStatus;
import org.honu.thrift.TChunk;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.THttpClient;

public class HTTPMessageSender extends MessageSender {

	private THttpClient httpClient = null;
	private TProtocol protocol = null;
	
	public HTTPMessageSender(MessageManager messageManager, boolean coreThread,
	    int queueThreshold, int senderTimeOut,
	    BlockingQueue<TChunk> chunkQueue) {
		super(messageManager,coreThread,queueThreshold,senderTimeOut,chunkQueue);	}

	 protected void closeConnection()  
	  throws CommunicationException {
     if (httpClient != null) {
       try {
         httpClient.close();
       } catch (Exception ignored) {
       }
       httpClient = null;
     }
     if (protocol != null) {
       protocol = null;
     }
     if (collector != null) {
       collector = null;
     }
	  }
	 
	protected void openConnection() throws CommunicationException {
		
		try {
			if (httpClient != null) {
				try {
					httpClient.close();
				} catch (Exception ignored) {
				}
				httpClient = null;
			}
			if (protocol != null) {
				protocol = null;
			}
			if (collector != null) {
				collector = null;
			}

			currentCollectorInfo = CollectorRegistry.getInstance().getCollector();
			if (currentCollectorInfo == null) {
				throw new CommunicationException("collector is null");
			}
			httpClient = new THttpClient(currentCollectorInfo.getHost() + ":" + currentCollectorInfo.getPort());
			
			httpClient.setConnectTimeout(SENDER_TIMEOUT);
			protocol = new TBinaryProtocol(httpClient);
			collector = new HonuCollector.Client(protocol);
			int status = collector.getStatus();
			if (status != ServiceStatus.ALIVE) {
				throw new RuntimeException("Collector is not alive! -- status:"
				    + status);
			}
		} catch (Throwable e) {
			collector = null;
			throw new CommunicationException(e);
		}

	}
}
