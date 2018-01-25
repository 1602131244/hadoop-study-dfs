package com.ibeifeng.hadoop.mapreduce.app;

import java.io.IOException;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class DataTotalMapReduce extends Configured implements Tool{
	/**
	 * 
	 * Mapper 实现类
	 * 
	 * 
	 */
	public static class DataTotalMapper extends
			Mapper<LongWritable, Text, Text, DataWritable> {
		private Text mapOutputKey = new Text();
		private DataWritable mapOutputValue = new DataWritable();
		
		
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			
			super.setup(context);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//TODO
			String lineValue = value.toString();
			//split 
			// 655209;1;796764372490213;804422938115889;6
			String[] strs = lineValue.split("\t");
			//set value
			String phoneNum = strs[1];
			int upPackNum = Integer.valueOf(strs[6]);
			int downPackNum = Integer.valueOf(strs[7]);
			int upPayLoad = Integer.valueOf(strs[8]);
			int downPayLoad = Integer.valueOf(strs[9]);
			
			mapOutputKey.set(phoneNum);
			mapOutputValue.set(upPackNum, downPackNum, upPayLoad, downPayLoad);
			
			//context write
			context.write(mapOutputKey, mapOutputValue);
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
	public static class DataTotalReducer 
			extends Reducer<Text, DataWritable, Text, DataWritable>{
		private DataWritable reduceOutputValue = new DataWritable();

		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		public void reduce(Text key, Iterable<DataWritable> values,
				Context context)
				throws IOException, InterruptedException {
			// TODO 
			int upPackNum = 0;
			int downPackNum = 0;
			int upPayLoad = 0;
			int downPayLoad = 0;
			
			for (DataWritable value : values) {
				upPackNum += value.getUpPackNum();
				downPackNum += value.getDownPackNum();
				upPayLoad += value.getUpPayLoad();
				downPayLoad += value.getDownPayLoad();
			}
			
			//set 
			reduceOutputValue.set(upPackNum, downPackNum, upPayLoad, downPayLoad);
			//context
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
		job.setMapperClass(DataTotalMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataWritable.class);
		
		
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
		job.setReducerClass(DataTotalReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataWritable.class);
		
		
		
		
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
	//	job.setUser("beifeng");
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
		args  =  new String[]{
				"",
				""
		};
		// mapreduce-default.xml , mapreduce-site.xml
		Configuration conf = new Configuration();
		
		//run mapreduce
		int status = ToolRunner.run(conf, new DataTotalMapReduce(), args);

		// exit program
		System.exit(status);
	}
}
