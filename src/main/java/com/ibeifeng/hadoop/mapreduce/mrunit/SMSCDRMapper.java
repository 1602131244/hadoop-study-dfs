package com.ibeifeng.hadoop.mapreduce.mrunit;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SMSCDRMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private Text mapOutputKey = new Text();
	private final static IntWritable mapOutputValue = new IntWritable(1);
	
	static enum CDRCounter {

		NonSMSCDR;

	};
	/**
	 * 1.CDRID;CDRType;Phone1;Phone2;SMS Status Code
 	 *	 655209;1;796764372490213;804422938115889;6
 	 *	 353415;0;356857119806206;287572231184798;4
 	 *	 835699;1;252280313968413;889717902341635;0
	 */
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String[] lineValue = value.toString().split(";");
		if (1 == Integer.valueOf(lineValue[1])) {
			mapOutputKey.set(lineValue[4]);
			context.write(mapOutputKey, mapOutputValue);
		} else {// CDR record is not of type SMS so increment the counter

			context.getCounter(CDRCounter.NonSMSCDR).increment(1);

		}
	}

}
