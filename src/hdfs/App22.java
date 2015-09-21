package hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class App22
{
	static final String PATH = "hdfs://hadoop0:9000/";
	static final String DIR = "/d1";
	static final String FILE = "/d1/hello";
	
	public static void main(String[] args) throws Exception, URISyntaxException
	{
		FileSystem fileSystem = getFileSystem();
		//mkdir
		//mkdir(fileSystem);
		//-put src des
		putData(fileSystem);
		
		//delete
		//fileSystem.delete(new Path(DIR), true);
	}

	private static void putData(FileSystem fileSystem) throws IOException,
			FileNotFoundException
	{
		FSDataOutputStream out = fileSystem.create(new Path(FILE));
		FileInputStream fileInputStream = new FileInputStream("E:/1wyl/Hadoop/test.txt");
		IOUtils.copyBytes(fileInputStream, out, new Configuration(), true);
	}

	private static void mkdir(FileSystem fileSystem) throws IOException
	{
		fileSystem.mkdirs(new Path(DIR));
	}

	private static FileSystem getFileSystem() throws IOException, URISyntaxException
	{
		return FileSystem.get(new URI(PATH), new Configuration());
	}
}
