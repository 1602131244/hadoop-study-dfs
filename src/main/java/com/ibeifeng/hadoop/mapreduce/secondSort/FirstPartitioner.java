package com.ibeifeng.hadoop.mapreduce.secondSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartitioner extends Partitioner<IntPairWritable, IntWritable> {

	@Override
	public int getPartition(IntPairWritable key, IntWritable value,
			int numPartitions) {
		// TODO Auto-generated method stub
		return (Integer.valueOf(key.getFirst()).hashCode() & Integer.MAX_VALUE)
				% numPartitions;

	}
	
}
