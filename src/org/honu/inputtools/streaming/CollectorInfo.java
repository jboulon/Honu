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

public class CollectorInfo {
	public static final int DEFAULT_PORT = 9600;
	public static final String DEFAULT_HOST = "localhost";
	
	private String host = DEFAULT_HOST;
	private int port = DEFAULT_PORT;
	
	public CollectorInfo() {
	}

	public CollectorInfo(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public String getHost() {
  	return host;
  }

	public void setHost(String host) {
  	this.host = host;
  }

	public int getPort() {
  	return port;
  }

	public void setPort(int port) {
  	this.port = port;
  }

	@Override
  public String toString() {
	  return this.host + ":" + this.port;
  }
	
	
}
