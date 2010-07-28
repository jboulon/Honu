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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class ISO8601 {
	static private long   lastTime;
	static private char[] lastTimeString = new char[20];
	static Calendar calendar = Calendar.getInstance();

	public static StringBuilder format(Date date, StringBuilder sbuf) {

		long now = date.getTime();
		int millis = (int)(now % 1000);

		if ((now - millis) != lastTime) {
			// We reach this point at most once per second
			// across all threads instead of each time format()
			// is called. This saves considerable CPU time.
			synchronized(calendar) {

				// NOTE: 
				// java.util.Date is ALWAYS GMT
				// And we want everything in GMT
				calendar.setTime(date);
				calendar.setTimeZone(TimeZone.getTimeZone("GMT"));

				int start = sbuf.length();

				int year =  calendar.get(Calendar.YEAR);
				sbuf.append(year);

				String month;
				switch(calendar.get(Calendar.MONTH)) {
				case Calendar.JANUARY: month = "-01-"; break;
				case Calendar.FEBRUARY: month = "-02-";  break;
				case Calendar.MARCH: month = "-03-"; break;
				case Calendar.APRIL: month = "-04-";  break;
				case Calendar.MAY: month = "-05-"; break;
				case Calendar.JUNE: month = "-06-";  break;
				case Calendar.JULY: month = "-07-"; break;
				case Calendar.AUGUST: month = "-08-";  break;
				case Calendar.SEPTEMBER: month = "-09-"; break;
				case Calendar.OCTOBER: month = "-10-"; break;
				case Calendar.NOVEMBER: month = "-11-";  break;
				case Calendar.DECEMBER: month = "-12-";  break;
				default: month = "-NA-"; break;
				}
				sbuf.append(month);

				int day = calendar.get(Calendar.DAY_OF_MONTH);
				if(day < 10)
					sbuf.append('0');
				sbuf.append(day);

				sbuf.append('T');

				int hour = calendar.get(Calendar.HOUR_OF_DAY);
				if(hour < 10) {
					sbuf.append('0');
				}
				sbuf.append(hour);
				sbuf.append(':');

				int mins = calendar.get(Calendar.MINUTE);
				if(mins < 10) {
					sbuf.append('0');
				}
				sbuf.append(mins);
				sbuf.append(':');

				int secs = calendar.get(Calendar.SECOND);
				if(secs < 10) {
					sbuf.append('0');
				}
				sbuf.append(secs);

				sbuf.append(',');

				// store the time string for next time to avoid recomputation
				sbuf.getChars(start, sbuf.length(), lastTimeString, 0);
				lastTime = now - millis;
			}
		} else {
			sbuf.append(lastTimeString);
		}
		
		if (millis < 100) {
			sbuf.append('0');
		}
		if (millis < 10) {
			sbuf.append('0');
		}
		sbuf.append(millis);
		return sbuf;
	}
	
	/**
	 * Date in ISO8601 format:
	 * yyyy-MM-dd'T'HH:mm:ss,SSS
	 * @param dateStr
	 * @return
	 * @throws ParseException 
	 */
	public static long getTime(String dateStr) throws ParseException {
		synchronized(calendar) {
			calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
			DateFormat outdfm = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss,SSS");
			outdfm.setTimeZone(TimeZone.getTimeZone("GMT"));
			calendar.setTime(outdfm.parse(dateStr));
			return calendar.getTimeInMillis();
		}
	}
}
