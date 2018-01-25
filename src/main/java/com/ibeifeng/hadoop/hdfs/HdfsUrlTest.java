package com.ibeifeng.hadoop.hdfs;

import java.io.File;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

public class HdfsUrlTest {
	//让JAVA程序识别hdfs的URL
	static{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	public static void main(String[] args){
		InputStream instream = null;
		OutputStream outsream = null;
		try{
			String file = "hdfs://hadoop-yarn.beifeng.com:8020/user/beifeng/mr/wordcount/input/wc.input";
			URL fileUrl= new URL(file);
			
			instream=fileUrl.openStream();
			
			//IOUtils.copyBytes(instream,System.out,4096,false);
			outsream= new FileOutputStream(new File("f:/wc.input"));
			
			IOUtils.copyBytes(instream,outsream,4096,false);
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(instream); 
			IOUtils.closeStream(outsream); 
		}
	}
}
