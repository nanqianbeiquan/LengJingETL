package etl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import tools.TableFactory;

public class UpdateBianGengMapper2  extends TableMapper<Text, Text> {

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
		super.cleanup(context);
		dstTable.flushCommits();
		dstTable.close();
	}
	
   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException 
   	{
   		cnt++;
   		if(cnt%10000==0)
   		{
   			System.out.println("cnt:"+cnt);
   		}
    	String key=Bytes.toString(value.getRow());
    	String companyName=key.split("_")[0];
    	String tableId=key.split("_")[1];
    	
    	if(tableId.equals("05"))
    	{
    		String bianGengShiXiang=Bytes.toString(value.getValue(Bytes.toBytes("Changed_Announcement"), Bytes.toBytes("changedannouncement_events")));
    		if(bianGengShiXiang!=null && (bianGengShiXiang.contains("(") || bianGengShiXiang.contains(")")))
        	{
        		bianGengShiXiang=bianGengShiXiang.replace("(", "（").replace(")", "）");
        		Put put=new Put(value.getRow());
        		put.addColumn(Bytes.toBytes("Changed_Announcement"), Bytes.toBytes("changedannouncement_events"),Bytes.toBytes(bianGengShiXiang));
        		dstTable.put(put);
        	}
    		String bianGengRiQi=Bytes.toString(value.getValue(Bytes.toBytes("Changed_Announcement"), Bytes.toBytes("changedannouncement_date")));
    		String uniqueFlag=companyName+"|"+bianGengRiQi+"|"+bianGengShiXiang;
    		context.write(new Text(uniqueFlag), new Text(key));
    	}
    	else if(tableId.equals("12"))
    	{
    		String dengJiBianHao=Bytes.toString(value.getValue(Bytes.toBytes("Equity_Pledge"), Bytes.toBytes("chattelmortgage_registrationno")));
    		if(dengJiBianHao!=null && (dengJiBianHao.contains("(") || dengJiBianHao.contains(")")))
        	{
        		dengJiBianHao=dengJiBianHao.replace("(", "（").replace(")", "）");
        		Put put=new Put(value.getRow());
        		put.addColumn(Bytes.toBytes("Equity_Pledge"), Bytes.toBytes("chattelmortgage_registrationno"),Bytes.toBytes(dengJiBianHao));
        		dstTable.put(put);
        	}
    		String equitypledge_registrationno=Bytes.toString(value.getValue(Bytes.toBytes("Equity_Pledge"), Bytes.toBytes("equitypledge_registrationno")));
    		String uniqueFlag=companyName+"|"+equitypledge_registrationno;
    		context.write(new Text(uniqueFlag), new Text(key));
    	}
    	
   	}
}
