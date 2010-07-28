package org.honu.example.client;

import java.util.Random;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.honu.util.KeyValueSerialization;

public class HelloWorld {
  static Logger log = Logger.getLogger(HelloWorld.class);
  
  /**
   * @param args
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception {
   for(int i=0;i<10;i++) {
    log.info("HelloWorld msg_" + i); 
   }
   
   Random randomGenerator = new Random();
   KeyValueSerialization kv = new KeyValueSerialization();
   
   for(int i=0;i<10;i++) {
     kv.startMessage("HonuTestTable");
     kv.addKeyValue("key1", ((i%2==0)?"true":"false") );
     kv.addKeyValue("key2", i );
     kv.addKeyValue("key3", randomGenerator.nextInt(1000) );
     log.info(kv.generateMessage());
    }
   
   Thread.sleep(20*1000);
   LogManager.shutdown();
  }

}
