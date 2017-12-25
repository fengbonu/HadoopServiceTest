package com.mytest.HadoopService.job;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class HdfsOperation {

 /**
  * 上传文件到hdfs
  * @param localSrc 本地需上传的文件路径
  * @param dst hdfs地址
  * @throws FileNotFoundException
  * @throws IOException
  */
 public static void uploadToHdfs(String localSrc, String dst) throws FileNotFoundException,
   IOException {
  InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
  Configuration conf = new Configuration();
  FileSystem fs = FileSystem.get(URI.create(dst), conf);
  OutputStream out = fs.create(new Path(dst), new Progressable() {
   public void progress() {
    System.out.print(".");
   }
  });
  IOUtils.copyBytes(in, out, 4096, true);
 }

 /**
  * 从HDFS上读取文件
  */
 private static void readFromHdfs() throws FileNotFoundException,IOException {
  String dst = "hdfs://192.10.5.76:9000/user/root/input0/a.txt"; 
  Configuration conf = new Configuration(); 
  FileSystem fs = FileSystem.get(URI.create(dst), conf);
  FSDataInputStream hdfsInStream = fs.open(new Path(dst));
  OutputStream out = new FileOutputStream("/home/li");
  byte[] ioBuffer = new byte[1024];
  int readLen = hdfsInStream.read(ioBuffer);
  while(-1 != readLen){
   out.write(ioBuffer, 0, readLen); 
   readLen = hdfsInStream.read(ioBuffer);
  }
  out.close();
  hdfsInStream.close();
  fs.close();
 }
 
 /**
  * HDFS上删除文件
  * @throws FileNotFoundException
  * @throws IOException
  */
 private static void deleteFromHdfs() throws FileNotFoundException,IOException {
  String dst = "hdfs://master:9000/user/root/input0/a.txt"; 
  Configuration conf = new Configuration(); 
  FileSystem fs = FileSystem.get(URI.create(dst), conf);
  fs.deleteOnExit(new Path(dst));
  fs.close();
 }
 
    /**
     * 遍历HDFS上的文件和目录
     */
    private static void getDirectoryFromHdfs() throws FileNotFoundException,IOException {
      String dst = "hdfs://master:9000/user/root/input0"; 
      Configuration conf = new Configuration(); 
      FileSystem fs = FileSystem.get(URI.create(dst), conf);
      FileStatus fileList[] = fs.listStatus(new Path(dst));
      int size = fileList.length;
      for(int i = 0; i < size; i++){
          System.out.println("name:" + fileList[i].getPath().getName() + "\t\tsize:" + fileList[i].getLen());
      }
      fs.close();
  }

    /**
     * main函数
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
  try {
//   uploadToHdfs();
   readFromHdfs();
   deleteFromHdfs();
   getDirectoryFromHdfs();
  } catch (Exception e) {
   // TODO Auto-generated catch block
   System.out.println(2);
   e.printStackTrace();
  }
  finally {
   System.out.println("SUCCESS");
   System.out.println(3);
  }
 }

}
