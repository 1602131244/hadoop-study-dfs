package com.ibeifeng.hadoop.mapreduce.mrunit;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SMSCDRReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	private IntWritable outputValue = new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum = 0;

		for (IntWritable value : values) {

			sum += value.get();

		}
		//set
		outputValue.set(sum);
		//write
		context.write(key,outputValue);
	}
	
}
