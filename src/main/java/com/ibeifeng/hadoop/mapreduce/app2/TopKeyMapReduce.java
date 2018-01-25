package com.ibeifeng.hadoop.mapreduce.app2;

import java.io.IOException;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * MapReduce 编程模板
 * 
 * 
 */
public class TopKeyMapReduce extends Configured implements Tool{
	/**
	 * 
	 * Mapper 实现类
	 * 
	 * 
	 */
	public static class TopKeyMapper extends
			Mapper<LongWritable, Text, LongWritable, NullWritable> {
		//存储最值， 初始化最小值
		
		Long topKey = Long.MIN_VALUE;
		
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
			//validate
			if(null == lineValue){
				return ;
				
			}
			long tempValue = Long.valueOf(lineValue.trim());
			
			//comparator
			if (topKey < tempValue){
				topKey = tempValue;
			}
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			
			context.write(new LongWritable(topKey), NullWritable.get());
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
		job.setMapperClass(TopKeyMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		
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
	//	job.setReducerClass(ModuleReducer.class);
	//	job.setOutputKeyClass(LongWritable.class);
	//	job.setOutputValueClass(Text.class);
		
		//set reduce number
		job.setNumReduceTasks(0);

		
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
		
		// mapreduce-default.xml , mapreduce-site.xml
		Configuration conf = new Configuration();
		
		//run mapreduce
		int status = ToolRunner.run(conf, new TopKeyMapReduce(), args);

		// exit program
		System.exit(status);
	}
}
