package old;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import combine.WordCountApp;

public class OldAPP
{
	static final String INPUT_PATH = "hdfs://wyl:9000/hello.txt";
	static final String OUT_PATH = "hdfs://wyl:9000/out";
	/**
	 * �Ķ���
	 * 1.����ʹ��Job������ʹ��JobConf
	 * 2.��İ�������ʹ��mapreduce������ʹ��mapred
	 * 3.����ʹ��job.waitForCompletion(true)�ύ��ҵ������ʹ��JobClient.runJob(job);
	 * 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		final Path outPath = new Path(OUT_PATH);
		if(fileSystem.exists(outPath)){
			fileSystem.delete(outPath, true);
		}
		
		final JobConf job = new JobConf(conf , WordCountApp.class);
		//1.1ָ����ȡ���ļ�λ������
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		//ָ����ζ������ļ����и�ʽ�����������ļ�ÿһ�н����ɼ�ֵ��
		//job.setInputFormatClass(TextInputFormat.class);
		
		//1.2 ָ���Զ����map��
		job.setMapperClass(MyMapper.class);
		//map�����<k,v>���͡����<k3,v3>��������<k2,v2>����һ�£������ʡ��
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(LongWritable.class);
		
		//1.3 ����
		//job.setPartitionerClass(HashPartitioner.class);
		//��һ��reduce��������
		//job.setNumReduceTasks(1);
		
		//1.4 TODO ���򡢷���
		
		//1.5 TODO ��Լ
		
		//2.2 ָ���Զ���reduce��
		job.setReducerClass(MyReducer.class);
		//ָ��reduce���������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//2.3 ָ��д��������
//		FileOutputFormat.setOutputPath(job, OUT_PATH);
		FileOutputFormat.setOutputPath(job, outPath);
		//ָ������ļ��ĸ�ʽ����
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		//��job�ύ��JobTracker����
		JobClient.runJob(job);
	}
	
	static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable>
	{
		public void map(LongWritable k1, Text v1,
				OutputCollector<Text, LongWritable> collector, Reporter reporter)
				throws IOException
		{
			String[] splited = v1.toString().split("\t");
			for (String word : splited)
			{
				collector.collect(new Text(word), new LongWritable(1));
			}
		}
	}
	
	static class MyReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable>
	{
		public void reduce(Text k2, Iterator<LongWritable> v2s,
				OutputCollector<Text, LongWritable> collector, Reporter reporter)
				throws IOException
		{
			long times = 0L;
			
			while (v2s.hasNext())
			{
				long temp = v2s.next().get();
				
				times += temp;
			}
			
			collector.collect(k2, new LongWritable(times));
		}
		
	}
}
