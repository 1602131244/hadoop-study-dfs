package com.ibeifeng.hadoop.mapreduce.den;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class JobDenMapReduce extends Configured implements Tool{

	
	/**
	 * 
	 * countJob
	 *    input:
	 *    		/user/beifeng/mr/depen/count/input
	 *    output:
	 *    		/user/beifeng/mr/depen/count/output
	 *    
	 *    
	 * topkeyJob   
	 * 
	 * 	  input:
	 *    		/user/beifeng/mr/depen/count/input
	 *    output:
	 *    		/user/beifeng/mr/depen/topkey/output
	 */
	//Driver
	 
	public int run(String[] args) throws Exception {
		//create job 
		Configuration countConf =new Configuration();
		
		Job countJob =Job.getInstance(countConf,"countJob");
		
		//set count Job
		//................................
		
		FileOutputFormat.setOutputPath(countJob, new Path(
				"/user/beifeng/mr/depen/count/output"));
		
//======================================================================================================
		
		// create job
		Configuration topkeyConf = new Configuration();

		Job topkeyJob = Job.getInstance(topkeyConf, "topkeyJob");

		// set topKey Job
		// ......................................

		FileInputFormat.addInputPath(topkeyJob, new Path(
				"/user/beifeng/mr/depen/count/output/part*"));

//============================================================================================
		
		//controlled job
		ControlledJob controlledJobCount =  new ControlledJob(countJob.getConfiguration());
		
		ControlledJob controlledJobTopKey = new ControlledJob(topkeyJob.getConfiguration());
		
		//add dependence
		
		controlledJobTopKey.addDependingJob(controlledJobCount);
		
		//job Control
		JobControl jobControl =new JobControl("count-topkey-mr");
		jobControl.addJob(controlledJobCount);
		jobControl.addJob(controlledJobTopKey);

		//run
		
		Thread jobControlThread = new Thread(jobControl);
		
		jobControlThread.start();
		
		while(!jobControl.allFinished()){
			Thread.sleep(500);
		}
		jobControl.stop();

		return 0;
	}
	
}
