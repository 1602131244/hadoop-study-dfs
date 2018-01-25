package com.ibeifeng.hadoop.mapreduce.input;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class NoSplitTextInputFormat extends TextInputFormat{

	@Override
	public boolean isSplitable(JobContext context, Path file) {
		return false;
	}

}
