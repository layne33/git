package hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class App2
{
	//FileSystem,全方位的对DFS的操作
	static final String PATH = "hdfs://hadoop0:9000/";
	static final String DIR = "/";
	static final String FILE = "/d1/hello";
	
	public static void main(String[] args) throws Exception, URISyntaxException
	{
		FileSystem fileSystem = getFileSystem();
		
		//创建文件夹 hadoop fs -mkdir /d1
//		mkdirs(fileSystem);
		//删除文件
//		remove(fileSystem);
		//上传文件 -put src des
//		putData(fileSystem);
		//下载文件 hadoop fs -get src des
//		getData(fileSystem);
		//遍历浏览文件
		list(fileSystem);
	}
	
	private static void list(FileSystem fileSystem) throws IOException
	{
		FileStatus[] listStatus = fileSystem.listStatus(new Path(DIR));
		for (FileStatus fileStatus : listStatus)
		{
			String idDir = fileStatus.isDir()?"文件夹":"文件";
			String permission = fileStatus.getPermission().toString();
			short replication = fileStatus.getReplication();
			long len = fileStatus.getLen();
			String path = fileStatus.getPath().toString();
			
			System.out.println(idDir + "\t" + permission + "\t" + replication + "\t" + len + "\t" + path);
		}
	}
	
	private static void getData(FileSystem fileSystem) throws IOException
	{
		FSDataInputStream in = fileSystem.open(new Path(FILE));
		IOUtils.copyBytes(in, System.out, 1024, true);
	}
	
	private static void putData(FileSystem fileSystem) throws IOException,
			FileNotFoundException
	{
		FSDataOutputStream out = fileSystem.create(new Path(FILE));
		FileInputStream in = new FileInputStream("E:/1wyl/Hadoop/test.txt");
		IOUtils.copyBytes(in, out, 1024, true);
	}
	
	private static void remove(FileSystem fileSystem) throws IOException
	{
		fileSystem.delete(new Path(DIR), true);
	}
	
	private static boolean mkdirs(FileSystem fileSystem) throws IOException
	{
		return fileSystem.mkdirs(new Path(DIR));
	}
	
	private static FileSystem getFileSystem() throws IOException, URISyntaxException
	{
		return FileSystem.get(new URI(PATH), new Configuration());
	}
}











