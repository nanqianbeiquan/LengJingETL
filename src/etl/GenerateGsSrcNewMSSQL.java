package etl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class GenerateGsSrcNewMSSQL {

	public void generateSqoopExportCmd(int seqId) throws ClassNotFoundException, SQLException
	{
		String sql1="select * from gongshang.lu_province_prefix";
		Class.forName( "org.apache.hive.jdbc.HiveDriver");
		Connection con = DriverManager.getConnection("jdbc:hive2://172.16.0.13:10000/default", "", "");
		Statement stmt = con.createStatement();
		
		ResultSet res1 = stmt.executeQuery(sql1);
		while(res1.next())
		{
			String provCode=res1.getString(1);
			String sqoopExportCmd=String.format("sqoop export "
					+ "--connect 'jdbc:sqlserver://172.16.0.26;username=qianjing;password=LENGjing1@34;database=GsSrc' "
					+ "--table GsSrc_%s_%d "
					+ "--export-dir /user/hive/warehouse/gongshang.db/gs_src_%d/prov_code=%s "
					+ "--input-fields-terminated-by '\\001'",provCode,seqId,seqId,provCode);
			System.out.println(sqoopExportCmd);
		}
	}
	
	public void generateCreateCmd(int seqId) throws SQLException, ClassNotFoundException
	{
		String sql1="select * from gongshang.lu_province_prefix";
		Class.forName( "org.apache.hive.jdbc.HiveDriver");
		Connection con = DriverManager.getConnection("jdbc:hive2://172.16.0.13:10000/default", "", "");
		Statement stmt = con.createStatement();
		
		ResultSet res1 = stmt.executeQuery(sql1);
		while(res1.next())
		{
			String provCode=res1.getString(1);
			
			String sql2=String.format("create table GsSrc_%s_%d (mc nvarchar(250)"
					+ ",xydm nvarchar(30)"
					+ ",zch nvarchar(30)"
					+ ",province nvarchar(50)"
					+ ",last_update_time datetime"
					+ ",update_status int)", provCode,seqId);
			
			System.out.println(sql2);
		}
	}
	
	public void generateIndexCmd(int seqId) throws SQLException, ClassNotFoundException
	{
		String sql1="select * from gongshang.lu_province_prefix";
		Class.forName( "org.apache.hive.jdbc.HiveDriver");
		Connection con = DriverManager.getConnection("jdbc:hive2://172.16.0.13:10000/default", "", "");
		Statement stmt = con.createStatement();
		
		ResultSet res1 = stmt.executeQuery(sql1);
		while(res1.next())
		{
			String provCode=res1.getString(1);
			String sql2=String.format("create index mc_idx on GsSrc_%s_%d (mc)", provCode,seqId);
			String sql3=String.format("create index update_status_idx on GsSrc_%s_%d (update_status)", provCode,seqId);
			System.out.println(sql2);
			System.out.println(sql3);
		}
	}
	
	public void generateTruncateCmd(int seqId) throws SQLException, ClassNotFoundException
	{
		String sql1="select * from gongshang.lu_province_prefix";
		Class.forName( "org.apache.hive.jdbc.HiveDriver");
		Connection con = DriverManager.getConnection("jdbc:hive2://172.16.0.13:10000/default", "", "");
		Statement stmt = con.createStatement();
		
		ResultSet res1 = stmt.executeQuery(sql1);
		while(res1.next())
		{
			String provCode=res1.getString(1);
			String sql2=String.format("truncate table GsSrc_%s_%d", provCode,seqId);
			System.out.println(sql2);
		}
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException
	{
		GenerateGsSrcNewMSSQL job=new GenerateGsSrcNewMSSQL();
		int seqId=4;
		job.generateCreateCmd(seqId);
		job.generateSqoopExportCmd(seqId);
		job.generateIndexCmd(seqId);
//		job.generateTruncateCmd(seqId);
	}
}
