package rpc;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class MyClient
{
	public static void main(String[] args)
	{
		//构造一个客户端代理对象，该代理对象实现了命名协议。代理对象会与制定地址的服务端通话
//		MyBiz proxy = (MyBiz)RPC.waitForProxy(MyBiz.class, 1234455, new InetSocketAddress(MyServer.ADDRESS, MyServer.PORT), new Configuration());
		
//		String result = proxy.hello("world");
		
//		System.out.println("客户端结果：" + result);
		
//		RPC.stopProxy(proxy);
	}
}
