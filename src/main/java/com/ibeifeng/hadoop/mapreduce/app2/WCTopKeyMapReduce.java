package com.ibeifeng.hadoop.mapreduce.app2;

import java.io.IOException;




import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WCTopKeyMapReduce extends Configured implements Tool{
	/**
	 * 
	 * Mapper 实现类
	 * 
	 * 
	 */
	public static class WCTopKeyMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable mapOutputValue = new IntWritable(1); 
		private Text mapOutputKey = new Text();
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			
			super.setup(context);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO

			String lineValue = value.toString();
			StringTokenizer st = new StringTokenizer(lineValue);
			while(st.hasMoreTokens()){
				String wordValue = st.nextToken();
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
	public static class WCTopKeyReducer 
			extends Reducer<Text, IntWritable, WCTopKeyWritable, NullWritable>{
		public static final int KEY = 3;
		
		//store value
		
		TreeSet<WCTopKeyWritable>  topSet = new TreeSet<WCTopKeyWritable>();
		
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
			
			WCTopKeyWritable wcTopKeyWritable = new WCTopKeyWritable(key.toString(),sum);
			
			//add element
			
			topSet.add(wcTopKeyWritable);
			
			//validate
			
			if(KEY < topSet.size()){
				topSet.remove(topSet.first());
			}
			
			

		}
		

		@Override
		public void cleanup(
				Context context)
				throws IOException, InterruptedException {
			for (WCTopKeyWritable values : topSet) {
				context.write(values, NullWritable.get());
			}
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
		job.setMapperClass(WCTopKeyMapper.class);
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
		job.setReducerClass(WCTopKeyReducer.class);
		job.setOutputKeyClass(WCTopKeyWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		
		
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
//		args = new String[] {
//				"hdfs://hadoop-yarn.beifeng.com:8020/user/beifeng/mr/wordcount/input",
//				"hdfs://hadoop-yarn.beifeng.com:8020/user/beifeng/mr/wordcount/output31"
//
//		};
		// mapreduce-default.xml , mapreduce-site.xml
		Configuration conf = new Configuration();
		
		//run mapreduce
		int status = ToolRunner.run(conf, new WCTopKeyMapReduce(), args);

		// exit program
		System.exit(status);
	}
}
