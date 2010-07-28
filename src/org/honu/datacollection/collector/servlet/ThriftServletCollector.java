package org.honu.datacollection.collector.servlet;

import org.honu.thrift.HonuCollector;
import org.apache.thrift.server.TServlet;
import org.honu.datacollection.collector.streaming.ThriftCollectorLockFreeImpl;

public class ThriftServletCollector  extends TServlet {	
	public ThriftServletCollector() throws Exception {
		super(new HonuCollector.Processor(new ThriftCollectorLockFreeImpl()));
  }

	/**
   * 
   */
  private static final long serialVersionUID = 2726365018591979800L;

  @Override
  public String getServletInfo() {
    return "Honu Thrift based Servlet Collector";
  }

}
