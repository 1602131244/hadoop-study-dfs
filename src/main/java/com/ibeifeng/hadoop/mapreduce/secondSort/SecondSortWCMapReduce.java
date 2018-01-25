package com.ibeifeng.hadoop.mapreduce.secondSort;

import java.io.IOException;

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
/**
 * 
 * 2017-9-21
 * sort (二次排序)
 * @author ganyangjie
 *
 */

public class SecondSortWCMapReduce extends Configured implements Tool{
	/**
	 * 
	 * Mapper 实现类
	 * 
	 * 
	 */
	public static class SecondSortWCMapper extends
			Mapper<LongWritable, Text, IntPairWritable, IntWritable> {
		private IntWritable mapOutputValue = new IntWritable(); 
		private IntPairWritable mapOutputKey = new IntPairWritable();
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
			String[] st = lineValue.split("\t");
			for (int i =0;i<st.length;i++) {
				//set output key and value
				String[]  inputString  =  st[i].split(" ");
				
	            mapOutputKey.set(Integer.valueOf(inputString[0]),Integer.valueOf(inputString[1]));
	            mapOutputValue.set(Integer.valueOf(inputString[1]));
				//output
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
	public static class SecondSortWCReducer 
			extends Reducer<IntPairWritable, IntWritable, IntWritable, IntWritable>{
		public IntWritable reduceOutputKey = new IntWritable();
		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		public void reduce(IntPairWritable key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			//set output key
			reduceOutputKey.set(key.getFirst());	
			//interator
			for(IntWritable value: values){
				
				//job ouput
				context.write(reduceOutputKey, value);
			}
			
			

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
		job.setMapperClass(SecondSortWCMapper.class);
		job.setMapOutputKeyClass(IntPairWritable.class);
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
		
		// 3) shuffle
		
		//partitioner
		job.setPartitionerClass(FirstPartitioner.class);
		
		
		//grouping
		job.setGroupingComparatorClass(GroupingComparator.class);
		
		
		
		//4) reducer class
		job.setReducerClass(SecondSortWCReducer.class);
		job.setOutputKeyClass(IntWritable.class);
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
				"hdfs://hadoop-yarn.beifeng.com:8020/user/beifeng/mr/sort/input",
				"hdfs://hadoop-yarn.beifeng.com:8020/user/beifeng/mr/sort/secondsort-output"

		};
		// mapreduce-default.xml , mapreduce-site.xml
		Configuration conf = new Configuration();
		
		//run mapreduce
		int status = ToolRunner.run(conf, new SecondSortWCMapReduce(), args);

		// exit program
		System.exit(status);
	}
}
