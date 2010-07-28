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

package org.apache.thrift.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

/**
 * <p>
 * A servlet for exposing Thrift services over HTTP. To use, create a subclass that
 * supplies a {@link TProcessor}. For example,
 * </p>
 * <pre>
 * public class CalculatorTServlet extends TServlet {
 *   public CalculatorTServlet() {
 *     super(new Calculator.Processor(new CalculatorHandler()));
 *   }
 * }
 * </pre>
 * <p>
 */

public class TServlet extends HttpServlet {
  private static final long serialVersionUID = 3088919109563813323L;
	protected TProcessor processor_ = null;
  protected TTransportFactory inputTransportFactory_ = new TTransportFactory();
  protected TTransportFactory outputTransportFactory_ = new TTransportFactory();
  protected TProtocolFactory inputProtocolFactory_ = new TBinaryProtocol.Factory();
  protected TProtocolFactory outputProtocolFactory_ = new TBinaryProtocol.Factory();

  public TServlet() {
  	
  }
  
  public TServlet(TProcessor processor) {
    processor_ = processor;
  }
  
  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    response.setContentType("application/x-thrift");
    InputStream in = request.getInputStream();
    OutputStream out = response.getOutputStream();

    TTransport client = new TIOStreamTransport(in, out);
    TProcessor processor = null;
    TTransport inputTransport = null;
    TTransport outputTransport = null;
    TProtocol inputProtocol = null;
    TProtocol outputProtocol = null;
    try {
      processor = processor_;
      inputTransport = inputTransportFactory_.getTransport(client);
      outputTransport = outputTransportFactory_.getTransport(client);
      inputProtocol = inputProtocolFactory_.getProtocol(inputTransport);
      outputProtocol = outputProtocolFactory_.getProtocol(outputTransport);
      while (processor.process(inputProtocol, outputProtocol)) {}
    } catch (TTransportException ttx) {
      // Client died, just move on
    } catch (TException tx) {
      tx.printStackTrace();
    } catch (Exception x) {
      x.printStackTrace();
    }

    if (inputTransport != null) {
      inputTransport.close();
    }
    
    if (outputTransport != null) {
      outputTransport.close();
    }

  }
}
