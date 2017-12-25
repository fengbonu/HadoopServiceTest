package com.mytest.HadoopService.job;

import java.io.IOException;  
  
import java.net.URI;  
  
   
  
import org.apache.hadoop.conf.Configuration;  
  
import org.apache.hadoop.fs.FileSystem;  
  
import org.apache.hadoop.mapred.JobConf;  
  
   
  
/** 
 
 * HDFS服务器 
 
 *  
 
 * @author hjn 
 
 * @version 1.0 2013-11-20 
 
 */  
  
public class HDFSServer {  
  
private static Configuration configuration;  
  
private static FileSystem fileSystem;  
  
private static final String HDFS_URL = "hdfs://192.168.1.210:8020";  
  
   
  
/** 
 
 * HDFS服务器读取初始化 
 
 */  
  
private static void init() {  
  
try {  
  
configuration= new JobConf(HDFSServer.class);  
  
fileSystem = FileSystem.get(URI.create(HDFS_URL), configuration);  
  
} catch (IOException e) {  
  
System.out.println("读取服务器失败");  
  
e.printStackTrace();  
  
}  
  
}  
  
public static FileSystem getFileSystem(){  
  
if(fileSystem==null){  
  
init();  
  
}  
  
return fileSystem;  
  
}  
  
   
  
}  