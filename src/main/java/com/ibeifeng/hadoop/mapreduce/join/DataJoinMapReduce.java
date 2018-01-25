package com.ibeifeng.hadoop.mapreduce.join;

import java.io.IOException;




import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

/**
 * 

 * 
 * 
 */
public class DataJoinMapReduce extends Configured implements Tool{
	/**
	 * 
	 * Mapper 实现类
	 * 
	 * <p>KEYIN:LongWritable<br>VALUEIN:Text
	 * <p>KEYOUT:LongWritable<br>VALUEOUT:Text
	 * 
	 */
	public static class DataJoinMapper extends
			Mapper<LongWritable, Text, LongWritable, DataJoinWritable> {
		//map output key
		private LongWritable mapOutputKey = new LongWritable();
		private DataJoinWritable mapOutputValue = new DataJoinWritable();
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			
			super.setup(context);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String inputValue = value.toString();
			//split
			String[] vals =  inputValue.trim().split(",");
			
			//get id
			Long id = Long.valueOf(vals[0].trim());
			
			//set map ouput key
			mapOutputKey.set(id);
			//get name
			String name = vals[1]; 
			//set customer
			if (3 == vals.length){
				String phone = vals[2];
				mapOutputValue.set("customer",//
						name + "," + phone);
			}else if (4 == vals.length){  //set order 
				Float price = Float.valueOf(vals[2]);
				String date = vals[3];
				mapOutputValue.set("order",//
						name + "," + price + "," + date);
				
			
			}
			// set map output
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
	 * <p>
	 * KEYIN:LongWritable<br>VALUEIN:Text
	 * <p>
	 * KEYOUT:LongWritable<br>VALUEOUT:Text
	 */
	public static class DataJoinReducer 
			extends Reducer<LongWritable, DataJoinWritable, NullWritable, Text>{

		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}

		@Override
		public void reduce(LongWritable key, Iterable<DataJoinWritable> values,
				Context context)
				throws IOException, InterruptedException {
			//customer info
			String customerInfo = null;
			//order info 
			List<String> orderList = new ArrayList<String>();
			for (DataJoinWritable value : values) {
				//get tag
				String tag = value.getTag();
				
				//validate
				if ("customer".equals(tag)) {
					customerInfo = value.getData();
				}else if ("order".equals(tag)) {
					orderList.add(value.getData());					
				}
				
			}
			
			//validate
			if(null == customerInfo || 0 == orderList.size()){
				return ;				
			}
			
			Text outputValue = new Text();
			//iterator
			
			for (String order : orderList) {
				//set value
				outputValue.set(key.toString() + "," + customerInfo + ","
						+ order);
				
				//reduce output
				context.write(NullWritable.get(), outputValue);
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
		job.setMapperClass(DataJoinMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(DataJoinWritable.class);
		
		
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
		job.setReducerClass(DataJoinReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		
		
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
		int status = ToolRunner.run(conf, new DataJoinMapReduce(), args);

		// exit program
		System.exit(status);
	}
}
