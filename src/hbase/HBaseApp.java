package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.sun.rowset.internal.Row;

public class HBaseApp
{
	public static String TABLE_NAME = "table1";
	public static String FAMLIY_NAME = "family1";
	public static String ROW_KEY = "rowkey1";

	public static void main(String[] args) throws Exception, Exception
	{
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.rootdir", "hdfs://hadoop0:9000/hbase");
		conf.set("hbase.zookeeper.quorum", "hadoop0");

		// 创建表，删除表使用HbaseAdmin
		HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
		
		createTable(hBaseAdmin);  //使用shift + alt + M快捷键来产生方法
		
//		deleteTable(hBaseAdmin);
		
		//插入记录，查询一条记录，遍历所有记录用HTable
		HTable hTable = new HTable(conf, TABLE_NAME);
		
//		putRecord(hTable);
		
//		getRecord(hTable);
		
//		scanTable(hTable);
		
		hTable.close();
		
	}

	private static void scanTable(HTable hTable) throws IOException
	{
		Scan scan = new Scan();
		ResultScanner scanner = hTable.getScanner(scan);
		for(Result result : scanner)
		{
			byte[] value = result.getValue(FAMLIY_NAME.getBytes(), "age".getBytes());
			System.out.println(result + "\t" + new String(value));
		}
	}

	private static void getRecord(HTable hTable) throws IOException
	{
		Get get = new Get(ROW_KEY.getBytes());
		Result result = hTable.get(get);
		byte[] value = result.getValue(FAMLIY_NAME.getBytes(), "age".getBytes());
		System.out.println(result + "\t" + new String(value));
	}

	private static void putRecord(HTable hTable) throws IOException
	{
		Put put = new Put(ROW_KEY.getBytes());
		put.add(FAMLIY_NAME.getBytes(), "age".getBytes(), "25".getBytes());
		hTable.put(put);
	}

	private static void deleteTable(HBaseAdmin hBaseAdmin) throws IOException
	{
		hBaseAdmin.disableTable(TABLE_NAME);
		hBaseAdmin.deleteTable(TABLE_NAME);
	}

	private static void createTable(HBaseAdmin hBaseAdmin) throws IOException
	{
		if (!hBaseAdmin.tableExists(TABLE_NAME))
		{
			HTableDescriptor tableDescriptor = new HTableDescriptor(TABLE_NAME);
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(
					FAMLIY_NAME);
			tableDescriptor.addFamily(hColumnDescriptor);
			hBaseAdmin.createTable(tableDescriptor);
		}
	}
}
