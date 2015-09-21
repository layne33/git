package hdfs;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class App1
{
	/**
	 * ���쳣��unknown host: hadoop0
	 * ԭ������δ����
	 */
	static final String PATH = "hdfs://wyl:9000/hello";
	
	public static void main(String[] args) throws Exception
	{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		
		//hadoop fs -ls hdfs://wyl:9000/
		URL url = new URL(PATH);
		
		InputStream inputStream = url.openStream();
		/*
		 * @param in	��ʾ������ 
		 * @param out 	��ʾ�����
		 * @param buffSize	��ʾ�����С
		 * @param close	��ʾ�ڴ���������Ƿ�ر���
		 */
		IOUtils.copyBytes(inputStream, System.out, 1024, true);
		
		
		
	}
}
