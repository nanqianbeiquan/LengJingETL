package tools;

import java.sql.SQLException;
import java.util.Arrays;

public class WriteJobStatus {

	public static void writeJobStatus(String jobName,String runDate,int status,String remark) throws ClassNotFoundException, SQLException
	{
		String sql0=String.format("delete from job_info.job_status where job_name='%s' and run_date='%s'", jobName,runDate);
//		System.out.println(sql0);
		MySQL.executeUpdate(sql0);
		String sql1=String.format("insert into job_info.job_status values('%s','%s',curtime(),%d,'%s')", jobName,runDate,status,remark);
		MySQL.executeUpdate(sql1);
//		MySQL.commit();
//		MySQL.close();
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException
	{
		WriteJobStatus.writeJobStatus("Test", "2016-04-28", 0, "Test");
//		String condition="50&";
//		String[] startAndStop=condition.split("&",-1);
//		System.out.println(Arrays.asList(startAndStop));
	}
}
