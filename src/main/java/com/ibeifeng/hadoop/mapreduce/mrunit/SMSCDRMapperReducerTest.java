package com.ibeifeng.hadoop.mapreduce.mrunit;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.ibeifeng.hadoop.mapreduce.mrunit.SMSCDRMapper.CDRCounter;

public class SMSCDRMapperReducerTest {
	// map Driver
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	// reduce Driver
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	// mapreduce Driver
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	
	@Before
	public void setUp(){
		//init mapDriver
		SMSCDRMapper mapper = new SMSCDRMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
		//init reduceDriver
		SMSCDRReducer reducer = new SMSCDRReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver();
		//
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		
	}
	
	@Test
	public void testMapper(){
		//input 
		mapDriver.withInput(new LongWritable(0L),new Text("655209;1;796764372490213;804422938115889;6"));
		//output
		mapDriver.withOutput(new Text("6"),new IntWritable(1));
		
		//run
		mapDriver.runTest();
		assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()

	              .findCounter(CDRCounter.NonSMSCDR).getValue());
	}
	
	@Test
	public void testReducer(){
		//input
		List<IntWritable> values = new ArrayList<IntWritable>();

	    values.add(new IntWritable(1));

	    values.add(new IntWritable(1));

	    reduceDriver.withInput(new Text("6"), values);
	    //output
	    reduceDriver.withOutput(new Text("6"), new IntWritable(2));
	    //run
	    reduceDriver.runTest();
	 }
	
	 @Test

	  public void testMapReduce() {
		 //input
	    mapReduceDriver.withInput(new LongWritable(), new Text(

	              "655209;1;796764372490213;804422938115889;6"));
	    //output
	    

	    mapReduceDriver.withOutput(new Text("6"), new IntWritable(1));

	    mapReduceDriver.runTest();

	  }
	
}
