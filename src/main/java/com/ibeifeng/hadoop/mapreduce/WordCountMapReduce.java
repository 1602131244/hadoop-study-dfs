package com.ibeifeng.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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



public class WordCountMapReduce extends Configured implements Tool{
	

	/*
	 * 
	 * 1):mapper class 内部类
	 */
	public static class WordCountMapper extends 
		Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable mapOutputValue = new IntWritable(1); 
		private Text mapOutputKey = new Text();
		/*
		 * <0, hadoop hdfs mapreduce>
		 *           |
		 *           |    StringTokenizer:hadoop,hdfs,mapreduce
		 * interator 
		 *           |
		 * <hadoop,1> , <hdfs,1> ,<mapreduce,1>
		 * 
		 */
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//第一种方式计数器
			
			context.getCounter("MAPREDUCER_COUNTERS", "MAPPER_INPUT_KEYVALUE_COUNTER").increment(1L);
			
			//============================================
			String lineValue = value.toString();
			/**
			 * String[] str = lineValue.split("");  //不推荐使用
			 * 
			 */
			//spilt
			StringTokenizer st = new StringTokenizer(lineValue);
			while(st.hasMoreTokens()){
				String wordValue = st.nextToken();
				//set output key
				mapOutputKey.set(wordValue);
				
				/**
				 * 
				 * context.write(//
				 *      new Text(wordValue), //
				 *      new IntWritable(1) //
				 * 
				 * );
				 */
				//output 
				context.write(mapOutputKey, mapOutputValue);
				
			}
			
		}							
	}
	
	
	/*
	 * 
	 * 
	 * 
	 * 2):reducer class 内部类
	 */
	public static class WordCountReducer extends 
		Reducer<Text, IntWritable, Text, IntWritable>{
		public IntWritable reduceOutputValue = new IntWritable();
        
        //第二种方式枚举类型计数器
        public static enum Counter{
        	REDUCER_OUTPUT_KEYVALUE_COUNTER
        }
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			// TODO 
			int sum = 0;
			//interator
			for(IntWritable value: values){
				sum += value.get();
			}
			
			//set output
			reduceOutputValue.set(sum);
			
			//job ouput
			context.write(key, reduceOutputValue);
			
			//=================================
			context.getCounter(Counter.REDUCER_OUTPUT_KEYVALUE_COUNTER).increment(1L);
		}	
	}
	/*
	 * <hdfs,list(1,2,1,1)>
	 *      |
	 *      |
	 *  interator
	 *      |  sum=1+2+1+1=5
	 *      
	 *  <hdfs,5>
	 *        
	 *      
	 * 
	 * 
	 */
	
	public int run(String[] args) throws Exception {
		
		
		Job job = Job.getInstance(super.getConf(), 
				WordCountMapReduce.class.getSimpleName()
		);
		
		job.setJarByClass(WordCountMapReduce.class);
		
		//set job
		//1.set input
		Path inputDir = new Path(args[0]);
		FileInputFormat.addInputPath(job,inputDir);
		
		//2.set mapper
		job.setMapperClass(WordCountMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//3.set reducer 
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//4.set output
		Path outputDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		//提交处理
		boolean isSuccess = job.waitForCompletion(true);

		return isSuccess ? 0 : 1;
	}
	
	
	/*
	 * 
	 * 3)driver
	 */
	public static void main(String[] args) throws Exception {
		//mapreduce-default.xml  ,  mapreduce-site.xml
		Configuration conf = new Configuration();
		int status = ToolRunner.run(conf, new WordCountMapReduce(), args);
		
		//exit program
		System.exit(status);;
		
	}
	
	
	
	
}
