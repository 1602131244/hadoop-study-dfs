package com.ibeifeng.hadoop.mapreduce.cache;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



@SuppressWarnings("deprecation")
public class DistributedCacheWCMapReduce extends Configured implements Tool{
	/**
	 * 
	 * Mapper 实现类
	 * 
	 * 
	 */
	public static class DistributedCacheWCMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		//cache
		List<String> list = new ArrayList<String>();
		
		 
		private final static IntWritable mapOutputValue = new IntWritable(1); 
		private Text mapOutputKey = new Text();
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			//read cache files
			//step1: get configuration
			Configuration conf =context.getConfiguration();
			//step2: get cache uri
			URI[] uris = DistributedCache.getCacheFiles(conf);
			//step3: path
			Path path =new Path(uris[0]);
			//step4: file system
			FileSystem fs =FileSystem.get(conf); 
			//step5: in stream
			InputStream inStream = fs.open(path);
				
			//step6: read data
			InputStreamReader  isr = new InputStreamReader(inStream);
			BufferedReader bf =new BufferedReader(isr);
			String line;
			while((line = bf.readLine()) != null){
				if(line.trim().length() > 0){
					//add element
					list.add(line);
				}
			}
			bf.close();
			isr.close();
			inStream.close();
			//fs.close();
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO

			String lineValue = value.toString();
			StringTokenizer st = new StringTokenizer(lineValue);
			while(st.hasMoreTokens()){
				String wordValue = st.nextToken();
				if(list.contains(wordValue)){
					continue;
				}
				//set output key
				mapOutputKey.set(wordValue);
			
				context.write(mapOutputKey, mapOutputValue);
			}
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			
			super.cleanup(context);
		}
	}

	/**
	 * 
	 * Reducer 实现类
	 * 
	 */
	public static class DistributedCacheWCReducer 
			extends Reducer<Text, IntWritable, Text, IntWritable>{
		public IntWritable reduceOutputValue = new IntWritable();
		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			//interator
			for(IntWritable value: values){
				sum += value.get();
			}
			
			//set output
			reduceOutputValue.set(sum);
			
			//job ouput
			context.write(key, reduceOutputValue);

		}
		

		@Override
		public void cleanup(
				Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
		}
		
	}
	/**
	 * 
	 * Driver :Job create,set,submit,run,monitor
	 */

	public int run(String[] args) throws Exception {
		// 1. get configuration
	    //Configuration configuration =new Configuration();
		Configuration configuration = this.getConf();
		
		// 2. create job
		Job job = this.parseInputAndOutput(this,configuration,args);
		
		
		
		// 4. set job
		
		
		
		//2) mapper class
		job.setMapperClass(DistributedCacheWCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		
		//3) shuffle
		//[1] partition
//		job.setPartitionerClass(HashPartitioner.class);
		
		//[2] sort
//		job.setSortComparatorClass(LongWritable.Comparator.class);
		
		//[3] optional,combiner
//		job.setCombinerClass(null);
		//[4] group
//		job.setGroupingComparatorClass(LongWritable.Comparator.class);
		
		//4) reducer class
		job.setReducerClass(DistributedCacheWCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		
		
		// 5. submit job
		boolean isSuccess = job.waitForCompletion(true);
		
		
		
		return isSuccess ? 0 : 1;
	}
	
	public Job parseInputAndOutput(Tool tool,Configuration configuration,String[] args) throws Exception{
		//validate args
		if(args.length !=2 ){
			System.out.println("Usage:  ==============="+tool.getClass().getSimpleName()+"[gentic  optional] <input><output>");
			ToolRunner.printGenericCommandUsage(System.err);
			return null;
		}
		
		// 2. create job
		Job job = Job.getInstance(configuration,tool.getClass().getSimpleName());
		
		// 3. set job sun class
		job.setJarByClass(tool.getClass());
		
		//1) input format
		Path inputPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inputPath);
		
		//5) output format
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		args = new String[] {
				"hdfs://hadoop-yarn.beifeng.com:8020/user/beifeng/mr/distributedcache/input",
				"hdfs://hadoop-yarn.beifeng.com:8020/user/beifeng/mr/distributedcache/output"

		};
		// mapreduce-default.xml , mapreduce-site.xml
		Configuration conf = new Configuration();
		
		//set distributed cache
		//=========================================
		URI uri = new URI("/user/beifeng/cachefile/cache.txt");
		DistributedCache.addCacheFile(uri, conf);
		
		//=========================================
		
		//run mapreduce
		int status = ToolRunner.run(conf, new DistributedCacheWCMapReduce(), args);

		// exit program
		System.exit(status);
	}
}
