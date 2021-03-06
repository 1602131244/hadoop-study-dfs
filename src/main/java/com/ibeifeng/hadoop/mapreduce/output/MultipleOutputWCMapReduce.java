package com.ibeifeng.hadoop.mapreduce.output;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * 2017-09-18
 * 需求：
 * 扩展Wordcount程序
 * 1）a-z开始的单词，az
 * 2）A-Z开始的单词 ， AZ
 * 3）0~9开始的单词， 09
 * 4）特殊字符，其他的字符开始的单词， other
 * 
 * @author Administrator
 *
 */
public class MultipleOutputWCMapReduce extends Configured implements Tool{
	/**
	 * 
	 * Mapper 实现类
	 * 
	 * 
	 */
	public static class MultipleOutputWCMapper extends
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
	public static class MultipleOutputWCReducer 
			extends Reducer<Text, IntWritable, Text, IntWritable>{
		//create MultipleOutputs instance
		
		private MultipleOutputs<Text, IntWritable> mop ;
		
		public IntWritable reduceOutputValue = new IntWritable();
		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			//new instance
			mop = new MultipleOutputs<Text,IntWritable>(context);
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
//			context.write(key, reduceOutputValue);
			
			//outputs validate
			String firstChar = key.toString().substring(0,1);
			if(firstChar.matches("[a-z]")){
				mop.write("az", key, reduceOutputValue);
			}else if (firstChar.matches("[A-Z]")) {
				mop.write("AZ", key, reduceOutputValue);
			}else if (firstChar.matches("[0-9]")) {
				mop.write("09", key, reduceOutputValue);
			}else {
				mop.write("other", key, reduceOutputValue);
			}

		}
		

		@Override
		public void cleanup(
				Context context)
				throws IOException, InterruptedException {
			mop.close();
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
		job.setMapperClass(MultipleOutputWCMapper.class);
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
		job.setReducerClass(MultipleOutputWCReducer.class);
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
		// set outputs
		MultipleOutputs.addNamedOutput(job, "az", TextOutputFormat.class,
				Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "AZ", TextOutputFormat.class,
				Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "09", TextOutputFormat.class,
				Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "other", TextOutputFormat.class,
				Text.class, IntWritable.class);
				
		return job;
	}
	public static void main(String[] args) throws Exception {
		args = new String[] {
				"hdfs://hadoop-yarn.beifeng.com:8020/user/beifeng/mr/wordcount/input",
				"hdfs://hadoop-yarn.beifeng.com:8020/user/beifeng/mr/wordcount/multiple-output"

		};
		// mapreduce-default.xml , mapreduce-site.xml
		Configuration conf = new Configuration();
		
		//run mapreduce
		int status = ToolRunner.run(conf, new MultipleOutputWCMapReduce(), args);

		// exit program
		System.exit(status);
	}
}
