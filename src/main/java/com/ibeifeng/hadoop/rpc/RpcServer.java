package com.ibeifeng.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class RpcServer implements IHello {
	public String hello(String name) {
		return "Hello "+name+"!";
	}
	public static void main(String[] args) throws Exception{
		Server server = new RPC.Builder(new Configuration())// 构建服务器
			.setProtocol(IHello.class)// 协议
			.setInstance(new RpcServer())//实例
			.setBindAddress("localhost") //绑定地址
			.setPort(10999) //端口号
			.build();
		
		server.start();
	}
}
