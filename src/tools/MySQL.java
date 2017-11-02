package tools;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQL {
	
	static Connection conn=null;
	static Statement statement=null;
	static String host="172.16.0.20";
	static String db="court_notice";
//	static String db="srd20160628";
	static String connectionURL=String.format("jdbc:mysql://%s:%d/%s?user=%s&password=%s&useUnicode=true&characterEncoding=utf-8&useSSL=false"
			,host,3306,db,"root","LENGjing1@34");
	static boolean autoCommit=true;
	static
	{
		System.out.println("建立mysql连接...");
		try {
			buildConnection();
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("mysql连接建立成功");
	}
	
	public static void buildConnection() throws ClassNotFoundException, SQLException
	{
		Class.forName("com.mysql.jdbc.Driver");
//		System.out.println("mysql连接建立成功");
		conn = DriverManager.getConnection(connectionURL);
		conn.setAutoCommit(autoCommit);
		statement=conn.createStatement();
	}
	
	public static ResultSet executeQuery(String sql) throws SQLException
	{
		return statement.executeQuery(sql);
	}
	
	public static int executeUpdate(String sql) throws SQLException
	{
//		System.out.println(sql);
		return statement.executeUpdate(sql);
	}

	public static void commit() throws SQLException
	{
		conn.commit();
	}
	
	public static void close() throws SQLException
	{
		conn.close();
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException
	{
		
		String sql="select \n"
+"kai_ting_ri_qi, \n"
+"an_you, \n"
+"an_hao, "
+"fa_yuan fa_yuan_ming_cheng, "
+"fa_ting shen_li_fa_ting, "
+"shen_pan_zhang zhu_shen_fa_guan, "
+"cheng_ban_bu_men cheng_ban_ting, "
+"concat('原告：',yuan_gao,'；被告：',bei_gao) dang_shi_ren "
+"from `hshfy` "
+"where add_time >='2016-01-01' and add_time<'2016-06-17'";
		
		executeQuery(sql);
	}
}
