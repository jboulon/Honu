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
package org.honu.services;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.honu.thrift.*;
import org.apache.thrift.TException;
public abstract class HonuServiceImpl implements HonuService.Iface{
	private long uptime = -1;
	protected Map<String, AtomicLong> counters = new HashMap<String, AtomicLong>();
	
	public HonuServiceImpl() {
		this.uptime = System.currentTimeMillis();
	}

	public abstract String getName() throws TException;

	public abstract String getVersion() throws TException ;
	
	public long getCounter(String key) throws TException {
		if (counters.containsKey(key)) {
			return counters.get(key).get();
		} else {
			return -1;
		}
	}

	public int getStatus() throws TException {
		return ServiceStatus.ALIVE;
	}

	public long getUptime() throws TException {
		return this.uptime;
	}

}
