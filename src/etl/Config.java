package etl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Config {

	public static String neo4jHost="172.16.0.17";
//	public static String neo4jHost="localhost";
	public static int neo4jPort=7474;
	public static String neo4jUser="neo4j";
	public static String neo4jPwd="1qaz@WSX3edc";
	
	public static int mysqlPort=3306;
	public static String mysqlHost="172.16.0.20";
	public static String mysqlUser="root";
	public static String mysqlPwd="LENGjing1@34";
	public static String mysqlDB="job_info";
	
	public static void loadConfig()
	{
		
	}
	
	public static String getNeo4jConnection()
	{
		return String.format("jdbc:neo4j://%s:%d/",neo4jHost,neo4jPort);
	}
	
	public static String getMysqlConnection()
	{
		return String.format("jdbc:mysql://%s:%d/%s?user=%s&password=%s&useUnicode=true&characterEncoding=utf-8&useSSL=false"
				,mysqlHost,mysqlPort,mysqlDB,mysqlUser,mysqlPwd);
	}
	
	public static void startNeo4jServer() throws IOException
	{
		String cmd=
				"PowerShell "
				+"Set-ExecutionPolicy -ExecutionPolicy RemoteSigned;"
				+ "Import-Module "
				+ System.getProperty("user.dir")+"\\plugin\\bin\\Neo4j-Management.psd1;"
				+ "'"+System.getProperty("user.dir")+"\\plugin'"+ " | Start-Neo4jServer -Console -Wait";
		System.out.println(cmd);
		Process process = Runtime.getRuntime().exec(cmd);
		BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
		String line=null;
		while((line=reader.readLine())!=null)
		{
			System.out.println('*'+line);
		}
	}
	
	public static void main(String[] args)
	{
		System.out.println(getMysqlConnection());
	}
}
