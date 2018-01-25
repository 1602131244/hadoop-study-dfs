package com.ibeifeng.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class HdfsUtils {
	/**
	 *get FileSystem实例
	 * 
	 * @return
	 */
	public static FileSystem getFs(){
		//第一步：获取 configuration
		Configuration conf= new Configuration();
								
		//第二步：获取filesystem                   
		FileSystem fs=null;
		try {
			fs = FileSystem.newInstance(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fs;
	}
	/**
	 * get LocalFileSystem实例
	 */
	public static LocalFileSystem getLocalFs(){
		//第一步：获取 configuration
		Configuration conf= new Configuration();
								
		//第二步：获取filesystem                   
		LocalFileSystem fs=null;
		try {
			fs = FileSystem.newInstanceLocal(conf);
		} catch (IOException e) {             
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fs;
	}
	/**
	 * get DistributedFileSystem实例
	 */
	public static DistributedFileSystem getDistributedFs(){
		//第一步：获取 configuration
		Configuration conf= new Configuration();
								
		//第二步：获取filesystem                   
		DistributedFileSystem fs=null;
		try {
			fs = (DistributedFileSystem)FileSystem.newInstance(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fs;
	}
}
