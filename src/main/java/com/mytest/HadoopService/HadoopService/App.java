package com.mytest.HadoopService.HadoopService;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.mytest.HadoopService.job.HdfsOperation;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		System.out.println("Hello World!");
		System.out.println("准备上传文件到hdfs。。。");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		System.out.println("开始执行上传时间：" + sdf.format(new Date()));
		long start = System.currentTimeMillis();
		try {
			// String localSrc = "/home/workspace/hjw/mytest.txt";
			// String dst = "hdfs://hadoop30:9000/audit/mytest.txt";
			// System.out.println("本地需上传文件路径：" + localSrc);
			// System.out.println("hdfs地址：" + dst);
			List<String> list = traverseGetFolder("/home/workspace/hjw/data/");
			System.out.println("子目录list=" + list.size());
//			 ExecutorService fixedThreadPool = Executors.newFixedThreadPool(500);
			List<Thread> threads = new ArrayList<Thread>();
			System.out.println("准备创建线程执行任务...");
			for (final String dataPath : list) {
//				 fixedThreadPool.execute(new Runnable() {
//				 public void run() {
//				 try {
//				 //获取目录下的所有文件
//				 List<String> listOfFile = traverseFolder(dataPath);
//				 for(String localSrc : listOfFile){
//				 String dst =
//				 "hdfs://hadoop30:9000/audit/"+localSrc.substring(localSrc.lastIndexOf("/")+1);
//				 HdfsOperation.uploadToHdfs(localSrc,dst);
//				 }
//				 }catch (Exception e){
//				 e.printStackTrace();
//				 }
//				 }
//				
//				 });
				
				Thread t = new Thread(new Runnable() {
					public void run() {
						try {
							// 获取目录下的所有文件
							List<String> listOfFile = traverseFolder(dataPath);
							for (String localSrc : listOfFile) {
								String dst = "hdfs://hadoop30:9000/audit/"
										+ localSrc.substring(localSrc.lastIndexOf("/") + 1);
								HdfsOperation.uploadToHdfs(localSrc, dst);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}

				});
				t.start();
				threads.add(t);
			}
//			 HdfsOperation.uploadToHdfs(localSrc,dst);
			for (Thread t : threads) {
				t.join(); // 用join()等待所有的线程。先后顺序无所谓，当这段执行完，肯定所有线程都结束了。
			}
			long end = System.currentTimeMillis();
			long temp = end - start;
			System.out.println("上传结束时间：" + sdf.format(new Date()) + "-->用 " + temp + " 毫秒");
			System.out.println("共用：" + (temp / 1000) + " 秒");
			System.out.println("上传文件到hdfs成功。。。");
		} catch (Exception e) {
			System.out.println("上传文件到hdfs失败，出现以下异常：");
			e.printStackTrace();
		}

	}

	/**
	 * 遍历目录下的文件
	 * 
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
						// System.out.println("文件夹:" + file2.getAbsolutePath());
						traverseFolder(file2.getAbsolutePath());
					} else {
						i++;
						// System.out.println("文件:" +
						// file2.getAbsolutePath()+";i="+i);
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
	 * 遍历目录下的子目录
	 * 
	 * @param path
	 */
	public static List<String> traverseGetFolder(String path) {
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
						// System.out.println("文件夹:" + file2.getAbsolutePath());
						list.add(file2.getAbsolutePath());
						traverseGetFolder(file2.getAbsolutePath());
					} else {
						// System.out.println("文件:" +
						// file2.getAbsolutePath()+";i="+i);
					}
				}
			}
			return list;
		} else {
			System.out.println("文件不存在!");
			return null;
		}
	}
}
