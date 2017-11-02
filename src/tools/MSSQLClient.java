package tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class MSSQLClient{

	public Connection conn;
//	public int fetchSize=1;
	public String driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	
	public String connectionURL;
	public String user;
	public String pwd;
	public boolean autoCommit;
	public Statement statement;
	public List<String> batchList=new ArrayList<String>();
	
//	private static String connectionURL = "jdbc:sqlserver://172.16.0.129:1433; DatabaseName=BaiduBaike"; 

	public MSSQLClient(String connectionURL,String user,String pwd,boolean autoCommit) throws ClassNotFoundException, SQLException
	{
		this.connectionURL=connectionURL;
		this.user=user;
		this.pwd=pwd;
		this.autoCommit=autoCommit;
		buildConnection();
	}
	
	public void buildConnection() throws ClassNotFoundException, SQLException
	{
		Class.forName(driverName);
		conn = DriverManager.getConnection(connectionURL, user, pwd);
		conn.setAutoCommit(autoCommit);
		statement=conn.createStatement();
	}

	public ResultSet executeQuery(String sql) throws SQLException
	{
		return statement.executeQuery(sql);
	}
	
	public int executeUpdate(String sql) throws SQLException
	{
		return statement.executeUpdate(sql);
	}
	
	public ResultSet execute(String sql) throws SQLException, ClassNotFoundException
	{
		PreparedStatement ps = conn.prepareStatement(sql,
				java.sql.ResultSet.TYPE_FORWARD_ONLY,java.sql.ResultSet.CONCUR_READ_ONLY);
		ps.setFetchSize(100); 
		return ps.executeQuery();
	}
	
	public void close() throws SQLException
	{
		conn.close();
	}
	
	public void commit() throws SQLException
	{
		conn.commit();
	}
	
	public static void test() throws SQLException, ClassNotFoundException
	{
		MSSQLClient msc=new MSSQLClient("jdbc:sqlserver://114.215.140.37:1433;DatabaseName=pachong",
				"likai","$qhz,gS*Z6S#3N\"*",false);
		String sql1="update test set col2=col1*10 where col1=2";
		msc.execute(sql1);
		
		String sql2="select @@ROWCOUNT";
		ResultSet res2 = msc.execute(sql2);
		
		while(res2.next())
		{
			System.out.println(res2.getInt(1));
		}
	
		
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException
	{
		test();
	}
}
