package etl;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import tools.TableFactory;

public class MergePersonNodesJob {

	public void loadDataToHbase() throws ClassNotFoundException, SQLException, IOException
	{
		Class.forName( "org.apache.hive.jdbc.HiveDriver");
		Connection con = DriverManager.getConnection("jdbc:hive2://172.16.0.13:10000/temp", "", "");
		Statement stmt = con.createStatement();
		ResultSet res = stmt.executeQuery("select * from temp.t_person_nodes_merge_result where personname!='1'");
		HTable dstTable=TableFactory.getTable("PersonNodesMergeResult");
		dstTable.setAutoFlushTo(false);
		dstTable.setWriteBufferSize(6*1024*1024);
		
		while(res.next())
		{
			String personName=res.getString(1);
			String companyName=res.getString(2);
			String idx=res.getString(3);
//			System.out.println(personName+"|"+companyName+"|"+idx);
			if(!personName.equals(""))
			{
				Put put=new Put(Bytes.toBytes(personName));
				put.addColumn(Bytes.toBytes("Company"), Bytes.toBytes(companyName), Bytes.toBytes(idx));
				dstTable.put(put);
			}
			
		}
		dstTable.flushCommits();
		dstTable.close();
		res.close();
	}
	
	public int run() throws IOException, SQLException, ClassNotFoundException, InterruptedException
	{
		int jobStatus=0;
		Configuration conf=HBaseConfiguration.create();
		Job job=Job.getInstance(conf);
		job.setJobName("自然人节点合并");
		job.setJarByClass(MergePersonNodesJob.class);
		job.setMapperClass(MergePersonNodesMapper.class);
		job.setReducerClass(MergePersonNodesReducer.class);
		job.setNumReduceTasks(20);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job,new Path("/user/hive/warehouse/temp.db/t_company_company_person"));
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path("/mrjob/MergePersonNodesJob"));
	    jobStatus=job.waitForCompletion(true)?0:1;
	    return jobStatus;
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, InterruptedException
	{
		MergePersonNodesJob job=new MergePersonNodesJob();
		job.run();
//		job.loadDataToHbase();
	}
}
