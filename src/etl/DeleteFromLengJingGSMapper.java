package etl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import tools.TableFactory;

public class DeleteFromLengJingGSMapper  extends Mapper<LongWritable , Text, NullWritable, NullWritable>{

	public Log logger=LogFactory.getLog(DeleteFromLengJingGSMapper.class);
	public String dstTableName="LengJingGS";
	public HTable dstTable;
	public String delimiter="\001";
	public int mcIdx=1;
	Filter filter=new KeyOnlyFilter();
	public int tableId=-1;
	List<Delete> deleteList=new ArrayList<Delete>();
	public void setup(Context context) throws IOException, InterruptedException
	{	
		super.setup(context);
		dstTable=(HTable) TableFactory.getTable(dstTableName);
		dstTable.setAutoFlushTo(false);
		dstTable.setWriteBufferSize(6*1024*1024);
		tableId=context.getConfiguration().getInt("tableId",-1);
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String vals[]=value.toString().split(delimiter,-1);
		String mc=vals[mcIdx];
		Scan scan=new Scan();
		scan.setFilter(filter);
		if(tableId==-1)
		{
			scan.setStartRow(Bytes.toBytes(mc));
			scan.setStopRow(Bytes.toBytes(mc+"_99"));
		}
		else
		{
			scan.setStartRow(Bytes.toBytes(mc+"_"+tableId));
			scan.setStopRow(Bytes.toBytes(mc+"_"+(tableId+1)));
		}
		scan.setBatch(100);
//		scan.setRowPrefixFilter(Bytes.toBytes(mc));
		
		ResultScanner resScan = dstTable.getScanner(scan);
		for(Result res=resScan.next();res!=null;res=resScan.next())
		{
			byte[] row=res.getRow();
//			logger.info(Bytes.toString(row));
			dstTable.delete(new Delete(row));
		}
		resScan.close();
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException 
	{
		super.cleanup(context);
		dstTable.flushCommits();
		dstTable.close();
	}
}
