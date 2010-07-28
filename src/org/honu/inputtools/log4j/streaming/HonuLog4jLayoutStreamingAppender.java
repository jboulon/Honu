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

import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;

public class HonuLog4jLayoutStreamingAppender  
  extends HonuLog4jStreamingAppender{
  
	public HonuLog4jLayoutStreamingAppender() {
		source = localHostAddr;
	}
	
	
	
	@Override
	protected void append(LoggingEvent event) {

		StringBuilder sb = this.stringBuilder.getSB();
		sb.setLength(0);
		sb.append(this.layout.format(event));

    if(layout.ignoresThrowable()) {
      String[] s = event.getThrowableStrRep();
      if (s != null) {
        int len = s.length;
        for(int i = 0; i < len; i++) {
          sb.append(s[i]);
          sb.append(Layout.LINE_SEP);
        }
      }
    }
    
		consumer.process(sb.toString());
	}
	
	@Override
  public boolean requiresLayout() {
		return true;
  }
}
