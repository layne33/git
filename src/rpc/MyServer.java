package rpc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class MyServer
{
	static final String ADDRESS = "localhost";
	static final int PORT = 12345;
	
	public static void main(String[] args) throws Exception
	{
		/**
		 * 构造一个RPC服务端
		 * 
		 */
		Server server = RPC.getServer(new MyBiz(), ADDRESS, PORT, new Configuration());
		server.start();
		
		
		
	}
}
