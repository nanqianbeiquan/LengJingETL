package etl;

import java.sql.SQLException;
import java.sql.Statement;

import tools.MSSQLClient;

public class FixShareholderTypeJob {

	public int run() throws ClassNotFoundException, SQLException
	{
		int jobStatus=0;
		MSSQLClient dbClient = new MSSQLClient(
				String.format("jdbc:sqlserver://172.16.0.26:1433;DatabaseName=pachong"),
				"likai", //user
				"d!{<kN5K38u-", //pwd
				true //autoCommit
				);
		Statement stat = dbClient.conn.createStatement();
		stat.execute("exec FixShareholderType");
		return jobStatus;
	}
}
