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

package org.honu.util;

import java.util.Iterator;
import java.util.Map;

public class KeyValueSerialization {
	public static final char fieldDelim = '\035';
	public static final char fieldEqual = '\002';
	public static final	String msgTypeKey = "DB";
	
	@SuppressWarnings("unchecked")
  private static class ThreadLocalStringBuilder extends ThreadLocal {
    public Object initialValue() {
      return new StringBuilder();
    } 
    public StringBuilder getSB() { 
      return (StringBuilder) super.get(); 
    }
  }
	private ThreadLocalStringBuilder stringBuilder = new ThreadLocalStringBuilder();
	
	public void startMessage(String msgType) {
		StringBuilder sb = stringBuilder.getSB();
		sb.setLength(0);
		sb.append(msgTypeKey).append(fieldEqual).append(msgType);
	}

	@SuppressWarnings("unchecked")
  public void startMessage(String msgType, Map map) {
		StringBuilder sb = stringBuilder.getSB();
		sb.setLength(0);
		sb.append(msgTypeKey).append(fieldEqual).append(msgType);
		Iterator it = map.keySet().iterator();
		Object key = null;
		while(it.hasNext()){
			key = it.next();
			sb.append(fieldDelim).append(key.toString()).append(fieldEqual).append(map.get(key).toString());
		}
	}
	
	
	public void addKeyValue(String key,String value) {
		StringBuilder sb = stringBuilder.getSB();
			sb.append(fieldDelim);
			sb.append(key).append(fieldEqual).append(value);
	}
	
	public void addKeyValue(String key,Object value) {
		StringBuilder sb = stringBuilder.getSB();
			sb.append(fieldDelim);
			sb.append(key).append(fieldEqual).append(value.toString());
	}
	
	public void addKeyValue(String key,int value) {
		StringBuilder sb = stringBuilder.getSB();
			sb.append(fieldDelim);
			sb.append(key).append(fieldEqual).append(value);
	}
	
	public void addKeyValue(String key,long value) {
		StringBuilder sb = stringBuilder.getSB();
			sb.append(fieldDelim);
			sb.append(key).append(fieldEqual).append(value);
	}
	
	public void addKeyValue(String key,float value) {
		StringBuilder sb = stringBuilder.getSB();
			sb.append(fieldDelim);
			sb.append(key).append(fieldEqual).append(value);
	}
	
	public String generateMessage() {
		return stringBuilder.getSB().toString();
	}
	
	public void reset() {
		stringBuilder.getSB().setLength(0);
	}
	
	@SuppressWarnings("unchecked")
  public static String generateMessageFromMap(String msgType, Map map,StringBuilder sb) {
		if (sb == null) {
			sb = new StringBuilder();
		}
		sb.setLength(0);
		sb.append(msgTypeKey).append(fieldEqual).append(msgType);
		Iterator it = map.keySet().iterator();
		Object key = null;
		while(it.hasNext()){
			key = it.next();
			sb.append(fieldDelim).append(key.toString()).append(fieldEqual).append(map.get(key).toString());
		}
		return sb.toString();
	}
}

