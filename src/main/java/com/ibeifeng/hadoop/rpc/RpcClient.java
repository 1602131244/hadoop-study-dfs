package com.ibeifeng.hadoop.rpc;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
public class RpcClient {
	
	public static void main(String[] args) throws Exception{
		
		IHello proxyHello = RPC.getProxy(//
				IHello.class,   
				1L, 
			    new InetSocketAddress("localhost",10999), 
				new Configuration());
		String nameString="hello bike";
		String result = proxyHello.hello(nameString);
		
		System.out.println("================"+result+"=================");
		 
		RPC.stopProxy(proxyHello);
	}
}
