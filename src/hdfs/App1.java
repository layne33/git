package hdfs;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class App1
{
	/**
	 * 抛异常：unknown host: hadoop0
	 * 原因：主机未解析
	 */
	static final String PATH = "hdfs://wyl:9000/hello";
	
	public static void main(String[] args) throws Exception
	{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		
		//hadoop fs -ls hdfs://wyl:9000/
		URL url = new URL(PATH);
		
		InputStream inputStream = url.openStream();
		/*
		 * @param in	表示输入流 
		 * @param out 	表示输出流
		 * @param buffSize	表示缓冲大小
		 * @param close	表示在传输结束后是否关闭流
		 */
		IOUtils.copyBytes(inputStream, System.out, 1024, true);
		
		
		
	}
}
