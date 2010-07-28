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

import org.honu.thrift.HonuCollector;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerSocket;

public class ThriftTHsHaCollector {
	private TNonblockingServerSocket transport = null;
	private THsHaServer server = null;
	private HonuCollector.Processor processor = null;
	
	public void start(int port) throws Exception {
		
		
		transport = new TNonblockingServerSocket(port);
		processor =  new HonuCollector.Processor(new ThriftCollectorLockFreeImpl());
		
		THsHaServer.Options serverOpt = new THsHaServer.Options() ;
		serverOpt.maxWorkerThreads=5;
		serverOpt.minWorkerThreads=5; 
		
		server = new THsHaServer(processor, transport,serverOpt);
		System.out.println("Server started on port:" + port);
		server.serve();
	}
	
	public static void main(String[] args) throws Exception {
		int port = 9600;
		if (args.length == 1) {
			port = Integer.parseInt(args[0]);
		}
		ThriftTHsHaCollector collector = new ThriftTHsHaCollector();
		collector.start(port);
	}


}
