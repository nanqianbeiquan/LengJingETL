package shuashuju;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import tools.TableFactory;

public class ShenJi {

	
	public static void run() throws IOException, SQLException, ClassNotFoundException
	{
		HTable table=TableFactory.getTable("LengJingSF");
		HTable table2=TableFactory.getTable("LengJingSFTemp");
		
		Class.forName( "org.apache.hive.jdbc.HiveDriver");
		Connection con = DriverManager.getConnection("jdbc:hive2://172.16.0.13:10000/graph_data", "", "");
		Statement stmt = con.createStatement();
		stmt.setFetchSize(10000);
		
//		String sql ="select * from dw.update_company";
		
		String sql = "select rowkey from temp.t_proceedings2";
		int i=0;
		ResultSet res = stmt.executeQuery(sql);
		List<Get> gets =new ArrayList<>();
		List<Delete> deletes =new ArrayList<>();
		while(res.next())
		{
			i++;
			String rowkey=res.getString(1);
			
			Get get=new Get(Bytes.toBytes(rowkey));
			get.addColumn(Bytes.toBytes("judgidentifier"), Bytes.toBytes("trialclass"));
			get.addColumn(Bytes.toBytes("judgidentifier"), Bytes.toBytes("companyname"));
			get.addColumn(Bytes.toBytes("judgidentifier"), Bytes.toBytes("judgmentid"));
			gets.add(get);
			if(i%500==0)
			{
				Result[] rl = table.get(gets);
				for(Result r:rl)
				{
					if(!r.isEmpty())
					{
						List<Cell> cells = r.listCells();
						if(cells.size()==1)
						{
//							System.out.println(rowkey);
							Delete delete=new Delete(r.getRow());
							deletes.add(delete);
						}
					}
				}
				if(deletes.size()>0)
				{
					table.delete(deletes);
					table2.delete(deletes);
				}
				System.out.println(">"+i);
				gets.clear();
				deletes.clear();
			}
			table.delete(deletes);
			table2.delete(deletes);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException
	{
		run();
	}
}
