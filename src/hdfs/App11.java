package hdfs;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;


public class App11
{
	static final String PATH = "hdfs://hadoop0:9000/a.txt";
	public static void main(String[] args) throws Exception
	{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		final URL url = new URL(PATH);
		final InputStream in = url.openStream();
		
		IOUtils.copyBytes(in, System.out, 1024, true);
	}
}
