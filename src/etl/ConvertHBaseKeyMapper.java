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

public class ConvertHBaseKeyMapper extends TableMapper<NullWritable, NullWritable> 
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
    	String key=Bytes.toString(value.getRow());
        if(key.contains("(") || key.contains(")"))
        {
        	String newKey=key.replace("(", "（").replace(")", "）");        	
        	Put put=new Put(Bytes.toBytes(newKey));
        	
        	Cell[] cells = value.rawCells();
        	for(Cell cell:cells)
        	{
        		byte[] family=Bytes.copy(cell.getFamilyArray(), cell.getFamilyOffset(),cell.getFamilyLength());
        		byte[] qualifier=Bytes.copy(cell.getQualifierArray(), cell.getQualifierOffset(),cell.getQualifierLength()); 
        		byte[] val=Bytes.copy(cell.getValueArray(), cell.getValueOffset(),cell.getValueLength());
        		put.addColumn(family, qualifier, val);
        	}
        	dstTable.put(put);
        	Delete delete=new Delete(value.getRow());
        	dstTable.delete(delete);
        }
   	}
}
