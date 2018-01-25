package com.ibeifeng.hadoop.hdfs;

import java.util.TreeSet;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        
    	TreeSet<Long> top =new TreeSet<Long>();
    	top.add(1L);
    	top.add(89L);
    	for (Long setLong :  top){
    		System.out.println(setLong);
    	}
    	
    	
    }
}
