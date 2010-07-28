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

package org.apache.hadoop.chukwa.datacollection.writer;


import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.conf.Configuration;

public interface ChukwaWriter {
  
  public static abstract class CommitStatus {}
  
  public static final CommitStatus COMMIT_OK = new CommitStatus() {};
  public static final CommitStatus COMMIT_FAIL = new CommitStatus() {};
  
  /**
   * COMMIT_PENDING should be returned if a writer has written data, but
   * this data may ultimately disappear. Contains a list of strings, format
   * unspecified, that agents can use to find out, eventually, if their data 
   * has committed.  String <n> corresponds to the nth chunk passed to add().
   * 
   *  At present, the format is <sinkfilename> <offset>, 
   *  where sinkfilename is the name of a sinkfile, without directory but with
   *  .done suffix, and offset is the last byte of the associated chunk.
   */
  public static class COMMIT_PENDING extends CommitStatus {
    public List<String> pendingEntries;
  
    public COMMIT_PENDING(int entries) {
      pendingEntries = new ArrayList<String>(entries);
    }
    
    public void addPend(String currentFileName, long dataSize) {
      pendingEntries.add(currentFileName+ " " + dataSize+"\n");
    }
  }
  
  /**
   * Called once to initialize this writer.
   * 
   * @param c
   * @throws WriterException
   */
  public void init(Configuration c) throws WriterException;

  /**
   * Called repeatedly with data that should be serialized.
   * 
   * Subclasses may assume that init() will be called before any calls to
   * add(), and that add() won't be called after close().
   * 
   * @param chunks
   * @return
   * @throws WriterException
   */
  public CommitStatus add(List<Chunk> chunks) throws WriterException;

  /**
   * Called once, indicating that the writer should close files and prepare
   * to exit.
   * @throws WriterException
   */
  public void close() throws WriterException;

}
