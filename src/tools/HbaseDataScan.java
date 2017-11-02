package tools;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDataScan {
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static void getTable(String tableName,String key) throws IOException
	{
		HTable table=TableFactory.getTable(tableName);
		Get get=new Get(Bytes.toBytes(key));
		Result res = table.get(get);
		for(Cell cell:res.rawCells())
		{
			String family=Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
			String qualifier=Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
			String value=Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
			System.out.println(sdf.format(new Date(cell.getTimestamp()))+">"+family+":"+qualifier+" -> "+value);
		}
	}
	
	public static void scanTable(String tableName,String startRow,String stopRow) throws IOException
	{
		HTable table=TableFactory.getTable(tableName);
		Scan scan=new Scan();
		if (startRow!=null)	scan.setStartRow(Bytes.toBytes(startRow));
		if (stopRow!=null) scan.setStopRow(Bytes.toBytes(stopRow));
		ResultScanner scanRes = table.getScanner(scan);

		int i=0;
		for(Result res=scanRes.next();res!=null;res=scanRes.next())
		{
			i++;
			System.out.println("<----------------"+Bytes.toString(res.getRow())+"---------> "+i);
			for(Cell cell:res.rawCells())
			{
				String family=Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
				String qualifier=Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
				String value=Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
				System.out.println(sdf.format(new Date(cell.getTimestamp()))+">"+family+":"+qualifier+" -> "+value);
			}
		}
		scanRes.close();
	}

	
	public static void main(String[] args) throws IOException
	{
//		getTable("LengJingSF","北京百度网讯科技有限公司_20_6cad53f3-969e-478c-b6f5-664bb29d0ca2");
//		scanTable("GS","北极绒（上海）纺织科技发展有限公司_01","北极绒（上海）纺织科技发展有限公司_02");
//		scanTable("PersonQueryDetail",null,null);
//		scanTable("LengJingSFTemp","枣庄新中兴实业有限责任公司_20","枣庄新中兴实业有限责任公司_21");
//		scanTable("LengJingSF","上海艺星医疗美容医院有限公司_20","上海艺星医疗美容医院有限公司_21");
		scanTable("LengJingSFTemp","上海艺星医疗美容医院有限公司_20","上海艺星医疗美容医院有限公司_21");
	}
}
