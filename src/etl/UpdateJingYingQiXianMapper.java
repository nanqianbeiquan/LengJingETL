package etl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;

import tools.TableFactory;

public class UpdateJingYingQiXianMapper extends TableMapper<NullWritable, NullWritable> 
{
	HTable dstTable;
	int cnt=0;
	public void setup(Context context) throws IOException, InterruptedException
	{	
		super.setup(context);
		Configuration conf = context.getConfiguration();
		String dstTableName=conf.get("dstTableName");
		dstTable=(HTable) TableFactory.getTable(dstTableName);
		dstTable.setAutoFlushTo(false);
		dstTable.setWriteBufferSize(6*1024*1024);
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		dstTable.flushCommits();
		dstTable.close();
		super.cleanup(context);
	}
	
   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException 
   	{
   		cnt++;
   		if(cnt%10000==0)
   		{
   			System.out.println("cnt:"+cnt);
   		}
   		String validityfrom=Bytes.toString(value.getValue(Bytes.toBytes("Registered_Info"),Bytes.toBytes("validityfrom")));
   		String validityto=Bytes.toString(value.getValue(Bytes.toBytes("Registered_Info"),Bytes.toBytes("validityto")));
   		
   		if(validityfrom!=null && validityto!=null && (validityfrom.equals("长期") || validityfrom.equals("永久") || validityfrom.compareTo(validityto)>0))
   		{
   			Put put=new Put(value.getRow());
   			put.addColumn(Bytes.toBytes("Registered_Info"),Bytes.toBytes("validityfrom"), Bytes.toBytes(validityto));
   			put.addColumn(Bytes.toBytes("Registered_Info"),Bytes.toBytes("validityto"), Bytes.toBytes(validityfrom));
   			dstTable.put(put);
   		}
   	}
}
