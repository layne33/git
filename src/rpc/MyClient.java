package rpc;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class MyClient
{
	public static void main(String[] args)
	{
		//����һ���ͻ��˴�����󣬸ô������ʵ��������Э�顣�����������ƶ���ַ�ķ����ͨ��
//		MyBiz proxy = (MyBiz)RPC.waitForProxy(MyBiz.class, 1234455, new InetSocketAddress(MyServer.ADDRESS, MyServer.PORT), new Configuration());
		
//		String result = proxy.hello("world");
		
//		System.out.println("�ͻ��˽����" + result);
		
//		RPC.stopProxy(proxy);
	}
}
