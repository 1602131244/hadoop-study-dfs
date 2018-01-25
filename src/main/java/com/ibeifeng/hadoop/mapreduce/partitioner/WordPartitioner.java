package com.ibeifeng.hadoop.mapreduce.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		String strValue = key.toString().substring(0, 1);
		if (strValue.matches("a-z")) {
			return 0 % numPartitions;
		} else if (strValue.matches("A-Z")) {
			return 1 % numPartitions;
		} else if (strValue.matches("0-9")) {
			return 2 % numPartitions;
		} else {
			return 3 % numPartitions;
		}

	}

}
