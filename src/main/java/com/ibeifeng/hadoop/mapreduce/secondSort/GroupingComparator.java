package com.ibeifeng.hadoop.mapreduce.secondSort;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator implements RawComparator<IntPairWritable> {

	public int compare(IntPairWritable o1, IntPairWritable o2) {
		return Integer.valueOf(o1.getFirst()).compareTo(
				Integer.valueOf(o2.getFirst()));
	}

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		// TODO Auto-generated method stub
		return WritableComparator.compareBytes(b1, s1, 4, b2, s2, 4);
	}

}
