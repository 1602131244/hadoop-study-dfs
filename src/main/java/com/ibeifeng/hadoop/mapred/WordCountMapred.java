package com.ibeifeng.hadoop.mapred;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountMapred extends Configured implements Tool{
	
	//Mapper class
	public static class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable mapOutputValue = new IntWritable(1);
		private Text mapOutputKey = new Text();
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			
			while(itr.hasMoreTokens()){
				String val = itr.nextToken();
				mapOutputKey.set(val);
				
				//output
				
				output.collect(mapOutputKey, mapOutputValue);
			}
		}
	}
	
	
	//Reducer class
	public static class WordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable outputValue = new IntWritable ();
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum =0;
			while(values.hasNext()){
				sum +=values.next().get();
			}
			//set 
			outputValue.set(sum);
			//output
			output.collect(key, outputValue);
		}
		
		
	}
	
	
	//Driver 
	public int run(String[] args) throws Exception {
		// get configuration
		Configuration conf = getConf();
		//create job
		JobConf job =new JobConf(conf);
		//set name
		job.setJobName(this.getClass().getSimpleName());
		job.setJarByClass(WordCountMapred.class);
		
		//set job 
		//1.input
		Path inPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPath);
		
		//2 map 
		job.setMapperClass(WordCountMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//3 reduce
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//4 output
		Path outPath =new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);
		
		
		//submit job
		RunningJob runJob = JobClient.runJob(job);
		
	
		return runJob.getJobState();
	}
	
	
	//main
	
	public static void main(String[] args) throws Exception {
		args = new String[] {
				"hdfs://hadoop-yarn.beifeng.com:8020/user/beifeng/mr/wordcount/input",
				"hdfs://hadoop-yarn.beifeng.com:8020/user/beifeng/mr/wordcount/mapred-output"

		};
		Configuration configuration =new Configuration();
		int status = ToolRunner.run(configuration, new WordCountMapred(), args);
		System.exit(status);
	}

}
