package com.ibeifeng.hadoop.hdfs;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * 
 * 实现本地文件上传到hdfs进行文件的合并
 * 1）本地文件系统LocalFileSystem\ HDFS DistirbutedFileSystem
 * 2) 获取要上传目录下的文件
 * 3）遍历文件，打开输入流，进行数据的读取
 * 4）打开HDFS要上传文件的一个输出流，用于写数据
 * 5） 关闭输入输出流
 * @author Administrator
 *
 */
public class PutMerge {
	public static void main(String[] args) {
	    args = new String[]{
	    	"f:/sites",//
	    	"/user/beifeng/putmerge/merge2.xml"//
	    };
		//第一步，fileSystem
	    
		LocalFileSystem localFileSystem = HdfsUtils.getLocalFs();
		FileSystem hdfs = HdfsUtils.getFs();
		
		//第二步：input/output path
		
		Path inPath = new Path(args[0]);
		Path outPath = new Path(args[1]);
		//第四步：outputStream
		OutputStream outputStream = null;
		
		
		try {
			outputStream=hdfs.create(outPath);
		    //第三部：获取上传目录下的文件
			
			FileStatus[] fileStatus = localFileSystem.listStatus(inPath);
			
			for (FileStatus fileStatu : fileStatus) {
				
				InputStream inputStream = localFileSystem.open(fileStatu.getPath());
				
				//读、写
				
				IOUtils.copyBytes(inputStream, outputStream, 4096 ,false);
				// 第五步 ：关闭输入流
				
				IOUtils.closeStream(inputStream);
				//print
				
				System.out.println("=============put===="+ //
						fileStatu.getPath().getName()+ "=======sucess============"
						);
			}
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(outputStream);
			System.out.println("============put sucess ===============");
		}
		
		
	}
}
