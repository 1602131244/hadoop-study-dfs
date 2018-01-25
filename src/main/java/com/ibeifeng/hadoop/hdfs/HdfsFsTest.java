package com.ibeifeng.hadoop.hdfs;

import java.io.File;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
public class HdfsFsTest {
	public static void main(String[] args) throws Exception{ 
		test();
		
	} 
	public static void load() throws Exception{
		FileSystem fs=HdfsUtils.getFs();
		
		LocalFileSystem localFileSystem = HdfsUtils.getLocalFs();
		InputStream inputStream = fs.open(new Path("/user/beifeng/mr/wordcount/input/put-wc.input"));
		OutputStream outputStream = localFileSystem.create(new Path("f:\\get-wc.input"));
		//System.setProperty("hadoop.home.dir","user/beifeng/mr/wordcount/input/put-wc.input" );
		IOUtils.copyBytes(inputStream,outputStream,4096,true);
		System.out.print("load() 成功!!!"); 
	}
	/**
	 * LocalFileSystem类向文件系统上传文件
	 * 
	 * @throws Exception
	 */
	public static void write2() throws Exception{
		FileSystem fs=HdfsUtils.getFs();
		
		LocalFileSystem localFileSystem = HdfsUtils.getLocalFs();
		//System.setProperty("hadoop.home.dir","user/beifeng/mr/wordcount/input/put-wc.input" );
		OutputStream outStream=fs.create(new Path(
				"hdfs://hadoop-yarn.beifeng.com/user/beifeng/mr/wordcount/input/put2-wc.input"
				)
		);
		InputStream inputStream = localFileSystem
				.open(new Path("f:\\wc.input"));
		IOUtils.copyBytes(inputStream,outStream,4096,true);
		System.out.print("write2() 成功!!!"); 
	}
	/**
	 * 在分布式文件系统创建目录
	 * @throws Exception
	 */
	public static void dirOpe() throws Exception{
		FileSystem fs =  HdfsUtils.getFs();
		boolean flag = fs.mkdirs(new Path("/user/beifeng/test"));
		System.out.println(flag);
		
		/*System.out.println(
				fs.listStatus(
						new Path("/user/beifeng/test")
				)
		);*/
		fs.delete(new Path("/user/beifeng/test"),false);
		System.out.println();
	}
	/*
	 * 
	 *获取集群上的主机名信息
	 */
	public static void clusterStatus() throws Exception{
		Configuration conf = new Configuration();
		String uri = "hdfs://hadoop-yarn.beifeng.com:8020";
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		//System.setProperty("hadoop.home.dir","user/beifeng/mr/wordcount/input/put-wc.input" );
		DistributedFileSystem dfs=(DistributedFileSystem)fs;
		//FsStatus fsStatus = dfs.getStatus();
		DatanodeInfo[] datanodeInfos=dfs.getDataNodeStats();
		String[] names = new String[datanodeInfos.length];
		
		/*for(DatanodeInfo datanodeInfo:datanodeInfos){
			System.out.println(datanodeInfo.getHostName());
		}*/
		for (int i = 0; i < datanodeInfos.length; i++) {
            names[i] = datanodeInfos[i].getHostName();
            System.out.println("node:" + i + ",name:" + names[i]);
        }
		
	}
	/**
	 *文件归属主机，带不出主机名。。。。
	 * @throws Exception
	 */
	public static void fileStatus() throws Exception{
		FileSystem fs=HdfsUtils.getFs();
		//System.setProperty("hadoop.home.dir","user/beifeng/mr/wordcount/input/put-wc.input" );
		OutputStream outStream=fs.create(new Path(
				"/user/beifeng/conf/core-site.xml"
				)
		);
		Path path =new Path("/user/beifeng/conf/core-site.xml");
		FileStatus fileStatus=fs.getFileStatus(path);
		
		BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 
				0, 
				fileStatus.getLen()
		);
		for(BlockLocation blockLocation : blockLocations){
			for(String host:blockLocation.getHosts()){
				System.out.println(host);
			}
		}
	}
	/**
	 * 上传到分布式系统上文件
	 * @throws Exception
	 */
	public static void write() throws Exception{
		FileSystem fs=HdfsUtils.getFs();
		//System.setProperty("hadoop.home.dir","user/beifeng/mr/wordcount/input/put-wc.input" );
		OutputStream outStream=fs.create(new Path(
				"hdfs://hadoop-yarn.beifeng.com/user/beifeng/mr/wordcount/input/put-wc.input"
				)
		);
		FileInputStream inStream=new FileInputStream(new File(
				"f:\\wc.input"));
		IOUtils.copyBytes(inStream,outStream,4096,true);
		System.out.print("Welcome to HDFS Java API!!!"); 
	}
	public static void test() throws Exception{
		//第一步：获取 configuration
		Configuration conf= new Configuration();
		//conf.set("dfs.permissions", "false");
		//第二步：获取filesystem
		FileSystem fs=FileSystem.get(new URI("/user/beifeng/mr/wordcount/input/put-wc.input"),
				conf
		);
		
		//第三部：file operate
		//fs.open(f, bufferSize)
		InputStream inStream=fs.open(new Path("/user/beifeng/mr/wordcount/input/put-wc.input"));
		IOUtils.copyBytes(inStream,System.out,4096,true);
	}
	public static void test2() throws Exception{
		//第一步：获取 configuration
		Configuration conf= new Configuration();
		//conf.set("fs.defaultFS","hdfs://hadoop-yarn.beifeng.com:8020");		
		//第二步：获取filesystem
		FileSystem fs=FileSystem.get(conf);
				
		//第三部：file operate
		//fs.open(f, bufferSize)
		InputStream inStream=fs.open(new Path("/user/beifeng/mr/wordcount/input/wc.input"));
		IOUtils.copyBytes(inStream,System.out,4096,true);
	}
	public static void test3() throws Exception{
		//第一步：获取 configuration
		Configuration conf= new Configuration();
						
		//第二步：获取filesystem                   
	    FileSystem fs=FileSystem.newInstance(conf);
		//第三部：file operate
		//fs.open(f, bufferSize)
		InputStream inStream=fs.open(new Path("/user/beifeng/mr/wordcount/input/wc.input"));
		IOUtils.copyBytes(inStream,System.out,4096,true);
	}
	public static void test4() throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.newInstance(//
				new URI("hdfs://hadoop-yarn.beifeng.com/user/beifeng/mr/wordcount/input/wc.input"),//
				conf//
			    //"beifeng"//
				);
		InputStream inStream=fs.open(new Path("/user/beifeng/mr/wordcount/input/wc.input"));
		IOUtils.copyBytes(inStream,System.out,4096,true);
	}
}
