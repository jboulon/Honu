package org.honu.inputtools.streaming;

import java.util.ArrayList;
import java.util.List;

public class CollectorRegistry {

	public static Object lock = new Object();
	public static CollectorRegistry collectorRegistry = null;
	
	private List<CollectorInfo> collectors  = new ArrayList<CollectorInfo>(10);
	private int collectorIndex = 0;
	
	private CollectorRegistry() {
	}
	
	public static CollectorRegistry getInstance() {
		
		if (collectorRegistry == null) {
		  synchronized(lock) {
		  	if (collectorRegistry == null) {
		  		collectorRegistry = new CollectorRegistry();
		  	}
		  }			
		}
		
	  return collectorRegistry;
	}
	
	public void setCollectors(String collectorsStr) {
	  String[] collectorArr = collectorsStr.split(",");
	  for (String coll : collectorArr) {
	    try {
	      CollectorInfo collectorInfo = new CollectorInfo();
	      collectorInfo.setHost(coll.substring(0, coll.lastIndexOf(':')));
	      collectorInfo.setPort(Integer.parseInt( coll.substring(coll.lastIndexOf(':')+1) ));
	      collectors.add(collectorInfo);
	    }catch (Exception e) {
	      System.err.println("Invalid list of collectors:" + e.getMessage() + " -- " + collectorsStr);
	      e.printStackTrace();
	    }
	  }
	}
	
	public synchronized CollectorInfo getCollector() {
	
		 if (collectors.isEmpty()) {
			 return null;
		 }
		 collectorIndex = (collectorIndex+1) % collectors.size();
		 return collectors.get(collectorIndex);
	}
	
	public int getCollectorCount() {
		return collectors.size();
	}
}
