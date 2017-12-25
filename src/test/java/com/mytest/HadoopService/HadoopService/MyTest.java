package com.mytest.HadoopService.HadoopService;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MyTest {
	public static int filelength;
	
	public static void main(String[] args) {
		double fileLength = traverseFolder2("C:\\Users\\Administrator\\Desktop\\hadoopservice\\newdata\\");
		//注：最好直接使用最基本单位的文件大小，转换成MB后的除法容易出错
		System.out.println(fileLength+" B");
//		while(fileLength < 1073741824){
//			fileLength = traverseFolder2("C:\\Users\\Administrator\\Desktop\\hadoopservice\\newdata\\");
//			
//		 	 ExecutorService fixedThreadPool = Executors.newFixedThreadPool(50);
//		 	fixedThreadPool.execute(new Runnable() {
//                public void run() {
//                    try {
//                    	List<String> list = traverseFolder("C:\\Users\\Administrator\\Desktop\\data\\");
//                    	long time = System.currentTimeMillis();
//                    	String pathOfs = "C:\\Users\\Administrator\\Desktop\\hadoopservice\\newdata\\"+time+"\\";
//    					File fileOfs = new File(pathOfs);
//    					if(!fileOfs.exists()){
//    						fileOfs.mkdirs();
//    					}
//    					System.out.println("目标路径："+fileOfs);
//                    	for(String path : list){
//            				FileInputStream in = null;
//            				FileOutputStream out = null;
//            				InputStreamReader ir = null;
//            				OutputStreamWriter ow = null;
//            				BufferedReader br = null;
//            				BufferedWriter bw = null;
//            				try {
//            					File file = new File(path);
//            					in = new FileInputStream(file);
//            					ir = new InputStreamReader(in,"utf-8");
//            					br = new BufferedReader(ir);
//            					
//            					out = new FileOutputStream(pathOfs+path.substring(path.lastIndexOf("\\")+1)+"."+time);
//            					ow = new OutputStreamWriter(out, "utf-8");
//            					bw = new BufferedWriter(ow);
//            					
//            					while(br.ready()){
//            						String str = br.readLine();
//            						System.out.println("数据："+str);
//            						String[] data = str.split(" ");
//            						bw.write(data[0]+"||||"+data[1]+"||||"+data[2]);
//            						bw.newLine();
//            					}
//            				} catch (Exception e) {
//            					e.printStackTrace();
//            				}finally{
//            					if(br != null){
//            						try {
//            							br.close();
//            						} catch (Exception e) {
//            						}
//            					}
//            					
//            					if(bw != null){
//            						try {
//            							bw.close();
//            						} catch (IOException e) {
//            						}
//            					}
//            				}
//            			}
//                    }catch (Exception e){
//                        e.printStackTrace();
//                    }
//                }
//            });
//		 	
////		 try {
////			Thread.sleep(2000);
////		} catch (InterruptedException e) {
////			e.printStackTrace();
////		}
//		}
		System.out.println("----ok-------fileLength="+fileLength+" B");
		
//		System.out.println("Hello World!");
//		FileInputStream in = null;
//		FileOutputStream out = null;
//		InputStreamReader ir = null;
//		OutputStreamWriter ow = null;
//		BufferedReader br = null;
//		BufferedWriter bw = null;
//		try {
//			File file = new File("C:\\Users\\Administrator\\Desktop\\testdata\\mytest.txt");
//			in = new FileInputStream(file);
//			ir = new InputStreamReader(in,"utf-8");
//			br = new BufferedReader(ir);
//			
//			out = new FileOutputStream("C:\\Users\\Administrator\\Desktop\\hadoopservice\\data\\mytest.txt");
//			ow = new OutputStreamWriter(out, "utf-8");
//			bw = new BufferedWriter(ow);
//			
//			while(br.ready()){
//				String str = br.readLine();
//				System.out.println("数据："+str);
//				String[] data = str.split("	");
//				System.out.println(Arrays.toString(data));
//				System.out.println(data[0]);
//				System.out.println(data[1]);
//				System.out.println(data[2]);
//				System.out.println(data[0]+" "+data[1]+" "+data[2]);
//				bw.write(data[0]+" "+data[1]+" "+data[2]);
//				bw.newLine();
//			}
//			System.out.println("---ok---");
//		} catch (Exception e) {
//			e.printStackTrace();
//		}finally{
//			if(br != null){
//				try {
//					br.close();
//				} catch (Exception e) {
//				}
//			}
//			
//			if(bw != null){
//				try {
//					bw.close();
//				} catch (IOException e) {
//				}
//			}
//		}
		
	}
	
	/**
	 * 遍历目录下的文件
	 * @param path
	 */
	public static List<String> traverseFolder(String path) {
		int i = 0;
		List<String> list = new ArrayList<String>();
        File file = new File(path);
        if (file.exists()) {
            File[] files = file.listFiles();
            if (files.length == 0) {
                System.out.println("文件夹是空的!");
                return null;
            } else {
                for (File file2 : files) {
                    if (file2.isDirectory()) {
//                        System.out.println("文件夹:" + file2.getAbsolutePath());
                        traverseFolder(file2.getAbsolutePath());
                    } else {
                    	i ++;
//                        System.out.println("文件:" + file2.getAbsolutePath()+";i="+i);
                        list.add(file2.getAbsolutePath());
                    }
                }
            }
            return list;
        } else {
            System.out.println("文件不存在!");
            return null;
        }
    }
	
	/**
	 * 遍历目录下的文件，计算目录下文件大小
	 * @param path
	 */
	public static int traverseFolder2(String path) {
        File file = new File(path);
        filelength = 0;
        if (file.exists()) {
            File[] files = file.listFiles();
            if (files.length == 0) {
//                System.out.println("文件夹是空的!");
                return 0;
            } else {
                for (File file2 : files) {
                    if (file2.isDirectory()) {
//                        System.out.println("文件夹:" + file2.getAbsolutePath());
                        traverseFolder2(file2.getAbsolutePath());
                    } else {
//                        System.out.println("文件:" + file2.getAbsolutePath());
                        filelength += file2.length();
                    }
                }
                
                return filelength;
            }
        } else {
            System.out.println("文件不存在!");
            return 0;
        }
    }
}
