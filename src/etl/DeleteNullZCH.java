package etl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import tools.TableFactory;

public class DeleteNullZCH {

	public DeleteNullZCH()
	{
		
	}
	
	public static int run2() throws IOException
	{
		File src=new File("data/NullZCH.txt");
		HTable table=TableFactory.getTable("GS");
		InputStreamReader read = new InputStreamReader(new FileInputStream(src));
        BufferedReader bufferedReader = new BufferedReader(read);
        String lineText = null;
        while((lineText = bufferedReader.readLine()) != null)
        {
        	Delete del=new Delete(Bytes.toBytes(lineText));
        	table.delete(del);
        }
        bufferedReader.close();
        table.close();
        return 1;
	}
	
	public static int run() throws IOException
	{
		int jobStatus=0;
		HTable table=TableFactory.getTable("GS");
		Filter filter=new KeyOnlyFilter();
		Scan scan=new Scan();
		scan.setFilter(filter);
		scan.setCaching(10000);
		ResultScanner resScanner = table.getScanner(scan);
		FileWriter fw=new FileWriter("data/NullZCH.txt");
		int cnt=0;
		for(Result res=resScanner.next();res!=null;res=resScanner.next())
		{
			String key=Bytes.toString(res.getRow());
			String[] vals = key.split("_",-1);
			String zch=vals[2];
			if(zch.equals(""))
			{
//				System.out.println(cnt+" -> "+key);
				fw.write(key+"\n");
			}
			cnt++;
			if(cnt%10000==0)
			{
				System.out.println(cnt);
			}
		}
		fw.close();
		return jobStatus;
	}
	
	public static void main(String[] args) throws IOException
	{
//		run();
		run2();
	}
}
