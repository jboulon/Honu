package org.honu.inputtools.converter;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkBuilder;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

public class CmdLineConverter {

  static String localHostAddr = null;
  
  static {
    try {
      localHostAddr = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      localHostAddr = "N/A";
    }
  }
  
  /**
   * @param args
   * @throws ClassNotFoundException 
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws ClassNotFoundException {
    
    if (args.length != 3) {
      System.out.println("java org.honu.inputtools.converter.CmdLineConverter <dataType> <codec> <outputFile>");
      System.out.println("codec: NONE , for uncompressed seqFile");
      System.out.println("codec: org.apache.hadoop.io.compress.GzipCodec , for GZIP compressed seqFile");
      System.out.println("codec: org.apache.hadoop.io.compress.LzoCodec , for LZO compressed seqFile");
      
      System.exit(-1);
    }
    
    String dataType = args[0];
    String codecClass = args[1];
    String outpFileName = args[2];
    
    if (codecClass.equalsIgnoreCase("none")) {
      codecClass = null;
    }
    int lineCount = 0;
    Path newOutputPath = null;
    
    try { 
      BufferedReader in = new BufferedReader(new InputStreamReader(System.in)); 

      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.getLocal(conf);
      newOutputPath = new Path(outpFileName);

      CompressionCodec codec = null;
      if (codecClass != null) {
        Class classDefinition =  Class.forName(codecClass);
        codec =  (CompressionCodec) ReflectionUtils.newInstance(classDefinition, conf);        
      }
      
        FSDataOutputStream newOutputStr = fs.create(newOutputPath);
      SequenceFile.Writer  seqFileWriter = null;
      
      if (codec != null) {
        seqFileWriter = SequenceFile.createWriter(conf, newOutputStr,
            ChukwaArchiveKey.class, ChunkImpl.class,
            SequenceFile.CompressionType.BLOCK, codec);
      } else {
        seqFileWriter = SequenceFile.createWriter(conf, newOutputStr,
            ChukwaArchiveKey.class, ChunkImpl.class,
            SequenceFile.CompressionType.NONE, codec);
      }
      
      
      String str = null; 
      ChunkBuilder cb = null;
     
      do
      { 
        str = in.readLine(); 
        
        if (str != null) {
          lineCount ++;
          if (cb == null) {
            cb = new ChunkBuilder();
          }
          cb.addRecord(str.getBytes());
          if (lineCount%300 == 0) {
            append(seqFileWriter,getChunk(cb,dataType));
            cb = null;
          }
        }   
      } while (str != null);
      
      if (cb != null) {
        append(seqFileWriter,getChunk(cb,dataType));
      }
      
      seqFileWriter.close();
      newOutputStr.close();
      
    } catch (Throwable e) { 
      e.printStackTrace();
      System.exit(-1);
    }
    System.out.println(new java.util.Date() + ", CmdLineConverter [" + dataType + "] [" +  newOutputPath + "], Total lineCount: " + lineCount);
    System.exit(0);
  }
  
  public static void append(SequenceFile.Writer  seqFileWriter, Chunk chunk) throws Throwable {
    ChukwaArchiveKey archiveKey = new ChukwaArchiveKey();
    archiveKey.setTimePartition(System.currentTimeMillis());
    archiveKey.setDataType(chunk.getDataType());
    archiveKey.setStreamName("CmdLineConverter");
    archiveKey.setSeqId(chunk.getSeqID());
    seqFileWriter.append(archiveKey, chunk);
  }
  
  public static Chunk getChunk(ChunkBuilder cb,String dataType) {
    Chunk c = cb.getChunk();
    c.setApplication("CmdLineConverter");
    c.setDataType(dataType);
    c.setSeqID(System.currentTimeMillis());
    c.setSource(localHostAddr);
    return c;
  }
}

