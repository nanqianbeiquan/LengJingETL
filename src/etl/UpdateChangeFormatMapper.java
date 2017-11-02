package etl;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;

import tools.TableFactory;

public class UpdateChangeFormatMapper extends TableMapper<NullWritable, NullWritable> 
{
	HTable dstTable;
	int cnt=0;
	SimpleDateFormat sdf1=new SimpleDateFormat("yyyy-MM-dd");
	SimpleDateFormat sdf2=new SimpleDateFormat("yyyy年M月d日");
	
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
    	String changeDt=Bytes.toString(value.getValue(Bytes.toBytes("Changed_Announcement"), Bytes.toBytes("changedannouncement_date")));
    	try 
    	{
    		if(changeDt!=null)
    		{
    			changeDt=sdf1.format(sdf2.parse(changeDt));
    			Put put=new Put(value.getRow());
    			put.addColumn(Bytes.toBytes("Changed_Announcement"), Bytes.toBytes("changedannouncement_date"), Bytes.toBytes(changeDt));
    			dstTable.put(put);
    		}
			
		} catch (ParseException e) 
    	{
			// TODO Auto-generated catch block
			return;
		}
   	}
}
