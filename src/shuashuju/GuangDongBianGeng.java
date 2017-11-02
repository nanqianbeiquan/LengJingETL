package shuashuju;

import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import tools.TableFactory;

public class GuangDongBianGeng {
	
	/**
	 * 广东省爬虫变更信息变更事项为英文的key，需要删除，变更时间为非标准格式的key，需要转换
	 */
	
	static SimpleDateFormat sdf1 = new SimpleDateFormat("MMM dd, yyyy hh:mm:ss a",Locale.ENGLISH);
	static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
	static Pattern alphaPattern=Pattern.compile("^[a-zA-Z]+$");
	
	public static boolean isAlpha(String s)
	{
		return s.matches("^[a-zA-Z]+$");
	}
	
	public static void run() throws IOException
	{
		HTable table=TableFactory.getTable("GS");
		Scan scan=new Scan();
		scan.addColumn(Bytes.toBytes("Changed_Announcement"), Bytes.toBytes("changedannouncement_date"));
		scan.addColumn(Bytes.toBytes("Changed_Announcement"), Bytes.toBytes("changedannouncement_events"));
		ResultScanner rs = table.getScanner(scan);
		
		FileWriter fw=new FileWriter("data/GuangDongBianGeng.txt");
		
		for(Result r:rs)
		{
			
			String rowkey=Bytes.toString(r.getRow());
			String[] infos = rowkey.split("_");
			String name=infos[0];
			String zch=infos[2];
			String provCode=null;
			if(zch.length()==18)
        	{
        		provCode=zch.substring(2,4);
        	}
			else if(zch.length()==13 || zch.length()==15)
        	{
        		provCode=zch.substring(0,2);
        	}
			if(provCode==null || !provCode.equals("44"))
			{
				continue;
			}
			
			String event=Bytes.toString(r.getValue(Bytes.toBytes("Changed_Announcement"), Bytes.toBytes("changedannouncement_events")));
			String dt=Bytes.toString(r.getValue(Bytes.toBytes("Changed_Announcement"), Bytes.toBytes("changedannouncement_date")));
			
			if(isAlpha(event))
			{
				Delete delete=new Delete(r.getRow());
				delete.addFamily(Bytes.toBytes("Changed_Announcement"));
				table.delete(delete);
				fw.write(name+"\n");
				fw.flush();
				System.out.println(name);
				continue;
			}
			else
			{
				String dt2=convertDate(dt);
				if(dt2!=null)
				{
					Put put=new Put(r.getRow());
					put.addColumn(Bytes.toBytes("Changed_Announcement"), Bytes.toBytes("changedannouncement_date"), Bytes.toBytes(dt2));
					table.put(put);
//					fw.write(name+"\n");
					fw.flush();
//					System.out.println("更新 "+Bytes.toString(r.getRow())+" " +dt+" 到 "+dt2);
				}
			}
			
		}
		fw.close();
		
	}
	
	public static String convertDate(String dt)
	{
		try
		{
			 return sdf2.format(sdf1.parse(dt));
		}
		catch (ParseException e)
		{
			return null;
		}
		
	}
	
	public static void main(String[] args) throws ParseException, IOException
	{
//		convertDate("Apr 21, 2016 12:00:00 AM");
//		System.out.println(convertDate("Apr 21, 2016 12:00:00 AM"));
//		System.out.println(convertDate("20160421"));
		run();
//		System.out.println(isAlpha("a合伙A"));
	}
	
}
