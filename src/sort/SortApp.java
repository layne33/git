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

public class SortApp
{
	static final String INPUT_PATH = "hdfs://wyl:9000/input";
	static final String OUT_PATH = "hdfs://wyl:9000/out";

	public static void main(String[] args) throws Exception
	{
		final Configuration configuration = new Configuration();

		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH),
				configuration);
		if (fileSystem.exists(new Path(OUT_PATH)))
		{
			fileSystem.delete(new Path(OUT_PATH), true);
		}

		final Job job = new Job(configuration, SortApp.class.getSimpleName());

		// 1.1 ָ�������ļ�·��
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		// ָ���ĸ���������ʽ�������ļ�
		job.setInputFormatClass(TextInputFormat.class);

		// 1.2ָ���Զ����Mapper��
		job.setMapperClass(MyMapper.class);
		// ָ�����<k2,v2>������
		job.setMapOutputKeyClass(NewK2.class);
		job.setMapOutputValueClass(LongWritable.class);

		// 1.3 ָ��������
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);

		// 1.4 TODO ���򡢷���

		// 1.5 TODO ����ѡ���ϲ�

		// 2.2 ָ���Զ����reduce��
		job.setReducerClass(MyReducer.class);
		// ָ�����<k3,v3>������
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);

		// 2.3 ָ�����������
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		// �趨����ļ��ĸ�ʽ����
		job.setOutputFormatClass(TextOutputFormat.class);

		// �Ѵ����ύ��JobTrackerִ��
		job.waitForCompletion(true);
	}

	static class MyMapper extends
			Mapper<LongWritable, Text, NewK2, LongWritable>
	{
		protected void map(
				LongWritable key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, NewK2, LongWritable>.Context context)
				throws java.io.IOException, InterruptedException
		{
			final String[] splited = value.toString().split("\t");
			final NewK2 k2 = new NewK2(Long.parseLong(splited[0]),
					Long.parseLong(splited[1]));
			final LongWritable v2 = new LongWritable(Long.parseLong(splited[1]));
			context.write(k2, v2);
		};
	}

	static class MyReducer extends
			Reducer<NewK2, LongWritable, LongWritable, LongWritable>
	{
		protected void reduce(
				NewK2 k2,
				java.lang.Iterable<LongWritable> v2s,
				org.apache.hadoop.mapreduce.Reducer<NewK2, LongWritable, LongWritable, LongWritable>.Context context)
				throws java.io.IOException, InterruptedException
		{
			context.write(new LongWritable(k2.first), new LongWritable(
					k2.second));
		};
	}

	/**
	 * �ʣ�Ϊʲôʵ�ָ��ࣿ ����Ϊԭ����v2���ܲ������򣬰�ԭ����k2��v2��װ��һ�����У���Ϊ�µ�k2
	 *
	 */
	static class NewK2 implements WritableComparable<NewK2>
	{
		Long first;
		Long second;

		public NewK2()
		{
		}

		public NewK2(long first, long second)
		{
			this.first = first;
			this.second = second;
		}

		@Override
		public void readFields(DataInput in) throws IOException
		{
			this.first = in.readLong();
			this.second = in.readLong();
		}

		@Override
		public void write(DataOutput out) throws IOException
		{
			out.writeLong(first);
			out.writeLong(second);
		}

		/**
		 * ��k2��������ʱ������ø÷���. ����һ�в�ͬʱ�����򣻵���һ����ͬʱ���ڶ�������
		 */
		@Override
		public int compareTo(NewK2 o)
		{
			final long minus = this.first - o.first;
			if (minus != 0)
			{
				return (int) minus;
			}
			return (int) (this.second - o.second);
		}

		@Override
		public int hashCode()
		{
			return this.first.hashCode() + this.second.hashCode();
		}

		@Override
		public boolean equals(Object obj)
		{
			if (!(obj instanceof NewK2))
			{
				return false;
			}
			NewK2 oK2 = (NewK2) obj;
			return (this.first == oK2.first) && (this.second == oK2.second);
		}
	}

}
