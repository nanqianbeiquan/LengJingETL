package etl;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import tools.JobConfig;
import tools.WriteJobStatus;

public class DeleteFromLengJingGSJob {

	public String database="ods";
	public String srcTableName="pachong_registered_info";
	public String DstTableName="LengJingGS";
	public String dt=null;
	
	public Path getSrcPath()
	{
		String src="/user/hive/warehouse/"+database+".db/"+srcTableName;
		if(dt!=null)
		{
			src=src+"/dt="+dt;
		}
		return new Path(src);
	}
	
	public int run(JobConfig jobConf) throws IOException, ClassNotFoundException, InterruptedException, SQLException
	{
		int jobStatus=0;
//		database=jobConf.getString("database");
//		srcTableName=jobConf.getString("srcTableName");
		dt=jobConf.getString("dt");
		Configuration conf=new Configuration();
		conf.set("mapreduce.input.fileinputformat.split.minsize", Integer.toString((128*1024*1024)/4));
		conf.set("mapreduce.input.fileinputformat.split.maxsize", Integer.toString((128*1024*1024)/4));
//		conf.set("srcTableName", srcTableName);
		if(jobConf.hasProperty("tableId"))
		{
			conf.setInt("tableId", jobConf.getInteger("tableId"));
		}
		Job job=Job.getInstance(conf);
		job.setJobName("Delete stale Data from LengJinfGS(dt='"+dt+"')");
		job.setJarByClass(DeleteFromLengJingGSJob.class);
		job.setMapperClass(DeleteFromLengJingGSMapper.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job,getSrcPath());
		job.setOutputFormatClass(NullOutputFormat.class);
		jobStatus=job.waitForCompletion(true)?0:1;
		
		WriteJobStatus.writeJobStatus(jobConf.jobName, dt, jobStatus, job.getCounters().toString());
		
		return jobStatus;
	}
	
}
