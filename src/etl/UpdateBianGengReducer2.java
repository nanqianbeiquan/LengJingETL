package etl;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class UpdateBianGengReducer2 extends TableReducer<Text,Text,Delete> {

	int cnt=0;

	public void setup(Context context) throws IOException, InterruptedException
	{	
		super.setup(context);
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		super.cleanup(context);
	}
	
   	public void reduce(Text row, Iterable<Text> values, Context context) throws IOException, InterruptedException 
   	{
   		Iterator<Text> iterator = values.iterator();
//   		String uniqueFlag=row.toString();
   		iterator.next();
   		while(iterator.hasNext())
   		{
   			String rowkey2=iterator.next().toString();
   			context.write(null, new Delete(Bytes.toBytes(rowkey2)));
   		}
   		
   	}
}
