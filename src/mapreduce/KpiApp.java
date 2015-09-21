package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class KpiApp
{
	static final String INPUT_PATH = "hdfs://wyl:9000/wlan";
	static final String OUT_PATH = "hdfs://wyl:9000/out";

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		final Path outPath = new Path(OUT_PATH);
		if (fileSystem.exists(outPath))
		{
			fileSystem.delete(outPath, true);
		}
		
		final Job job = new Job(conf,
				KpiApp.class.getSimpleName());
		// 1.1 ָ�������ļ�·��
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		// ָ���ĸ���������ʽ�������ļ�
		job.setInputFormatClass(TextInputFormat.class);

		// 1.2ָ���Զ����Mapper��
		job.setMapperClass(MyMapper.class);
		// ָ�����<k2,v2>������
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(KpiWritable2.class);

		// 1.3 ָ��������
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);

		// 1.4 TODO ���򡢷���

		// 1.5 TODO ����ѡ���ϲ�

		// 2.2 ָ���Զ����reduce��
		job.setReducerClass(MyReducer.class);
		// ָ�����<k3,v3>������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KpiWritable2.class);

		// 2.3 ָ�����������
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		// �趨����ļ��ĸ�ʽ����
		job.setOutputFormatClass(TextOutputFormat.class);

		// �Ѵ����ύ��JobTrackerִ��
		job.waitForCompletion(true);
	}

	static class MyMapper extends Mapper<LongWritable, Text, Text, KpiWritable2>
	{
		protected void map(
				LongWritable key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, KpiWritable2>.Context context) throws IOException, InterruptedException
		{
			final String[] splited = value.toString().split("\t");
			final String msisdn = splited[1];
			final Text k2 = new Text(msisdn);
			final KpiWritable2 v2 = new KpiWritable2(splited[6], splited[7], splited[8], splited[9]);
			context.write(k2, v2);
		};
	}

	static class MyReducer extends
			Reducer<Text, KpiWritable2, Text, KpiWritable2>
	{
		/**
		 * @param k2
		 *            ��ʾ�����ļ��в�ͬ���ֻ�����
		 * @param v2s
		 *            ��ʾ���ֻ����ڲ�ͬʱ�ε������ļ���
		 */
		protected void reduce(
				Text k2,
				java.lang.Iterable<KpiWritable2> v2s,
				org.apache.hadoop.mapreduce.Reducer<Text, KpiWritable2, Text, KpiWritable2>.Context context)
				throws IOException, InterruptedException
		{
			long upPackNum = 0L;
			long downPackNum = 0L;
			long upPayLoad = 0L;
			long downPayLoad = 0L;

			for (KpiWritable2 kpiWritable : v2s)
			{
				upPackNum += kpiWritable.upPackNum;
				downPackNum += kpiWritable.downPackNum;
				upPayLoad += kpiWritable.upPayLoad;
				downPayLoad += kpiWritable.downPayLoad;
			}

			final KpiWritable2 v3 = new KpiWritable2(upPackNum + "", downPackNum
					+ "", upPayLoad + "", downPayLoad + "");
			context.write(k2, v3);
		};
	}
}

class KpiWritable implements Writable
{
	long upPackNum;
	long downPackNum;
	long upPayLoad;
	long downPayLoad;

	public KpiWritable()
	{
	}

	public KpiWritable(String upPackNum, String downPackNum, String upPayLoad,
			String downPayLoad)
	{
		this.upPackNum = Long.parseLong(upPackNum);
		this.downPackNum = Long.parseLong(downPackNum);
		this.upPayLoad = Long.parseLong(upPayLoad);
		this.downPayLoad = Long.parseLong(downPayLoad);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		this.upPackNum = in.readLong();
		this.downPackNum = in.readLong();
		this.upPayLoad = in.readLong();
		this.downPayLoad = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(upPackNum);
		out.writeLong(downPackNum);
		out.writeLong(upPayLoad);
		out.writeLong(downPayLoad);
	}

	@Override
	public String toString()
	{
		return upPackNum + "\t" + downPackNum + "\t" + upPayLoad + "\t"
				+ downPayLoad;
	}
}
