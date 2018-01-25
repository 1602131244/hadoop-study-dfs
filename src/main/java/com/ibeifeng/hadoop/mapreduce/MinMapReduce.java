package com.ibeifeng.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * 
 * 
 */
public class MinMapReduce extends Configured implements Tool{
	/**
	 * 
	 * Driver :Job create,set,submit,run,monitor
	 */

	public int run(String[] args) throws Exception {
		// 1. get configuration
	    //Configuration configuration =new Configuration();
		Configuration configuration = this.getConf();
		
		// 2. create job
		Job job = Job.getInstance(configuration,this.getClass().getSimpleName());
		// 3. set job run class
		job.setJarByClass(this.getClass());
			
		// 4. set job
//		TextInputFormat
//		job.setInputFormatClass(cls);

		
		// 1)input format
		Path inputPath = new Path(args[0]);
		
		FileInputFormat.addInputPath(job, inputPath);
		
		//2) mapper class
//		job.setMapperClass(ModuleMapper.class);
//		job.setMapOutputKeyClass(LongWritable.class);
//		job.setMapOutputValueClass(Text.class);
		
		
		//3) shuffle
		//[1] partition
//		job.setPartitionerClass(HashPartitioner.class);
		
		//[2] sort
//		job.setSortComparatorClass(LongWritable.Comparator.class);
		
		//[3] optional,combiner
//		job.setCombinerClass(null);
		//[4] group
//		job.setGroupingComparatorClass(LongWritable.Comparator.class);
		
//		//4) reducer class
//		job.setReducerClass(ModuleReducer.class);
//		job.setOutputKeyClass(LongWritable.class);
//		job.setOutputValueClass(Text.class);
//		
		// 5) :output format
		
		Path outputPath =new Path(args[1]);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		
//		job.setOutputFormatClass(cls);
		// 5. submit job
		boolean isSuccess = job.waitForCompletion(true);
			
		return isSuccess ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		args = new String[]{
				"",
				""
		};
		// mapreduce-default.xml , mapreduce-site.xml
		Configuration conf = new Configuration();
		
		//run mapreduce
		int status = ToolRunner.run(conf, new MinMapReduce(), args);

		// exit program
		System.exit(status);
	}
}
