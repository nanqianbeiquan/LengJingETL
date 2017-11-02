package tools;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class TableFactory {

	public static Configuration HBaseConfig=null;
	public static Connection connection;

	public static HTable getTable(String tableName) throws IOException
	{
		System.setProperty("hadoop.home.dir", "E:\\EclipseProjects\\CDH5.6");
		if(HBaseConfig==null)
		{
			HBaseConfig = HBaseConfiguration.create(new Configuration());
		}
		HBaseConfig.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		Connection connection = ConnectionFactory.createConnection(HBaseConfig);
		HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
		return table;
	}
	
	public static void main(String[] args) throws IOException
	{
//		Get get=new Get(Bytes.toBytes(""));
		System.out.println("+++++++++++++++++++++");
		System.setProperty("hadoop.home.dir", "E:\\EclipseProjects\\CDH5.6");
		HTable table=getTable("LengJingSFTemp");
		System.out.println("---------------------");
//		Put put=new Put(Bytes.toBytes("成都市锦诚公路工程有限公司_20_b8aa6620-0365-46db-8035-5429ecb5d0fb"));
//		put.addColumn(Bytes.toBytes("judgidentifier"), Bytes.toBytes("casetype"), Bytes.toBytes("民事判决书"));
//		put.addColumn(Bytes.toBytes("judgidentifier"), Bytes.toBytes("litigationtype"), Bytes.toBytes("民事"));
//		put.addColumn(Bytes.toBytes("judgidentifier"), Bytes.toBytes("instrumenttype"), Bytes.toBytes("判决书"));
//		table.put(put);
//		table.flushCommits();
//		
//		System.out.println();
		
//		
		Scan scan=new Scan();
		scan.setStartRow(Bytes.toBytes("浙江维龙家居用品有限公司"));
		scan.setStopRow(Bytes.toBytes("浙江维龙家居用品有限公司_99"));
		ResultScanner scanRes = table.getScanner(scan);
		int i=0;
		for(Result res=scanRes.next();res!=null;res=scanRes.next())
		{
			System.out.println("---------------------------------"+Bytes.toString(res.getRow()));
			
			for(Cell cell:res.rawCells())
			{
				String family=Bytes.toString(cell.getFamily());
				String qualifier=Bytes.toString(cell.getQualifier());
				String value=Bytes.toString(cell.getValue());
				System.out.println(family+":"+qualifier+" -> "+value);
			}
//			if((i++)==100)
//			{
//				break;
//			}
		}
		scanRes.close();
	}
	
}
