package org.honu.datacollection.collector.streaming;

import org.apache.hadoop.conf.Configuration;
import org.honu.configuration.HonuConfigurationFactory;
import org.honu.datacollection.collector.servlet.ThriftServletCollector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.*;
import org.mortbay.jetty.servlet.*;
import org.mortbay.jetty.security.SslSocketConnector;

public class ThriftJettyCollector {
	static int THREADS = 120;
	public static Server jettyServer = null;

	public static void main(String[] args) throws Exception {
		 Configuration conf = HonuConfigurationFactory.getInstance().getCollectorConfiguration();
     
     THREADS = conf.getInt("chukwaCollector.http.threads", THREADS);

     // Set up jetty connector
     int portNum = conf.getInt("chukwaCollector.http.port", 9999);
     
     SelectChannelConnector jettyConnector = new SelectChannelConnector();
     jettyConnector.setLowResourcesConnections(THREADS - 10);
     jettyConnector.setLowResourceMaxIdleTime(1500);
     jettyConnector.setPort(portNum);
     
     int sslPortNum = conf.getInt("chukwaCollector.https.port", 9998);
     
     SslSocketConnector sslConnector =  new SslSocketConnector();
     sslConnector.setPort(sslPortNum);
     sslConnector.setMaxIdleTime(1500);
     sslConnector.setKeystore(conf.get("chukwaCollector.keystore"));
     sslConnector.setTruststore(conf.get("chukwaCollector.truststore"));
     sslConnector.setPassword(conf.get("chukwaCollector.password"));
     sslConnector.setKeyPassword(conf.get("chukwaCollector.keyPassword"));
     sslConnector.setTrustPassword(conf.get("chukwaCollector.trustPassword"));
     
     // Set up jetty server proper, using connector
     jettyServer = new Server(portNum);
     jettyServer.setConnectors(new Connector[] { jettyConnector,sslConnector });
     org.mortbay.thread.BoundedThreadPool pool = new org.mortbay.thread.BoundedThreadPool();
     pool.setMaxThreads(THREADS);
     jettyServer.setThreadPool(pool);

     
     // Add the collector servlet to server
     Context root = new Context(jettyServer, "/", Context.SESSIONS);
     root.addServlet(new ServletHolder(new ThriftServletCollector()), "/*");

     jettyServer.start();
     jettyServer.setStopAtShutdown(true);

     
	}
}
