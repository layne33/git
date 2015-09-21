package cmd;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountApp extends Configured implements Tool{
	static String INPUT_PATH = "";
	static String OUT_PATH = "";
	
	@Override
	public int run(String[] arg0) throws Exception {
		INPUT_PATH = arg0[0];
		OUT_PATH = arg0[1];
		
		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		final Path outPath = new Path(OUT_PATH);
		if(fileSystem.exists(outPath)){
			fileSystem.delete(outPath, true);
		}
		
		final Job job = new Job(conf , WordCountApp.class.getSimpleName());
		//������б���ִ�е����ܷ���
		job.setJarByClass(WordCountApp.class);
		
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
		FileOutputFormat.setOutputPath(job, outPath);
		//ָ������ļ��ĸ�ʽ����
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		//��job�ύ��JobTracker����
		job.waitForCompletion(true);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new WordCountApp(), args);
	}
	
	/**
	 * KEYIN	��k1		��ʾ�е�ƫ����
	 * VALUEIN	��v1		��ʾ���ı�����
	 * KEYOUT	��k2		��ʾ���г��ֵĵ���
	 * VALUEOUT	��v2		��ʾ���г��ֵĵ��ʵĴ������̶�ֵ1
	 */
	static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		protected void map(LongWritable k1, Text v1, Context context) throws java.io.IOException ,InterruptedException {
			final String[] splited = v1.toString().split("\t");
			for (String word : splited) {
				context.write(new Text(word), new LongWritable(1));
			}
		};
	}
	
	/**
	 * KEYIN	��k2		��ʾ���г��ֵĵ���
	 * VALUEIN	��v2		��ʾ���г��ֵĵ��ʵĴ���
	 * KEYOUT	��k3		��ʾ�ı��г��ֵĲ�ͬ����
	 * VALUEOUT	��v3		��ʾ�ı��г��ֵĲ�ͬ���ʵ��ܴ���
	 *
	 */
	static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		protected void reduce(Text k2, java.lang.Iterable<LongWritable> v2s, Context ctx) throws java.io.IOException ,InterruptedException {
			long times = 0L;
			for (LongWritable count : v2s) {
				times += count.get();
			}
			ctx.write(k2, new LongWritable(times));
		};
	}

		
}
