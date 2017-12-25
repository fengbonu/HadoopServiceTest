package com.mytest.HadoopService.HadoopService;

import java.io.File;

public class Test {
	
	public static int filelength;
	
//	public static void main(String[] args) {
//		String path = "C:\\Users\\Administrator\\Desktop\\audit\\DBAudit.bcp.2.1492766023.1492766023";
//		
//		if(path.lastIndexOf("\\") != -1){
//			path = path.substring(path.lastIndexOf("\\")+1);
//			System.out.println(path);
//		}
//	}
	
	public static void main(String[] args) {
		File file = new File("C:\\Users\\Administrator\\Desktop\\TT\\struts-STRUTS_2_3_32.zip");
		System.out.println(file.length());
		System.out.println(traverseFolder2("C:\\Users\\Administrator\\Desktop\\hadoopservice\\newdata\\"));
	}
	
	/**
	 * 遍历目录下的文件，计算目录下文件大小
	 * @param path
	 */
	public static int traverseFolder2(String path) {
        File file = new File(path);
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
            }
//            System.out.println(filelength);
            return filelength;
        } else {
            System.out.println("文件不存在!");
            return 0;
        }
    }
}
