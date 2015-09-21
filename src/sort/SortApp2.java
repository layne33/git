package sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class SortApp2
{
	static String INPUT_PATH = "hdfs://wyl:9000/input";
	static String OUT_PATH = "hdfs://wyl:9000/out";
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(new Path(OUT_PATH)))
		{
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		
		Job job = new Job(conf, SortApp2.class.getSimpleName());
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.waitForCompletion(true);
		
	}
	
	
	static class MyMapper extends
			Mapper<LongWritable, Text, LongWritable, LongWritable>
	{
		@Override
		protected void map(
				LongWritable key,
				Text value,
				Mapper<LongWritable, Text, LongWritable, LongWritable>.Context context)
				throws IOException, InterruptedException
		{
			String[] splited = value.toString().split("\t");
			
			LongWritable k2 = new LongWritable(Long.parseLong(splited[0]));
			LongWritable v2 = new LongWritable(Long.parseLong(splited[1]));
			
			context.write(k2, v2);
		}
	}
	
	static class MyReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>
	{
		@Override
		protected void reduce(
				LongWritable k2,
				Iterable<LongWritable> v2s,
				Reducer<LongWritable, LongWritable, LongWritable, LongWritable>.Context context)
				throws IOException, InterruptedException
		{
			for(LongWritable v2 : v2s)
			{
				context.write(k2, v2);
			}
		}
	}
}
