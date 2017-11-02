package neo4j;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import tools.JobConfig;
import tools.WriteJobStatus;

public class UpdateRiskModelJob {

	String parentDir="/user/hive/warehouse/riskmodel.db/risk_model_result";
	Configuration conf=new Configuration();
	String dt,lastDt;
	
	public Path getSrc()
	{
		return new Path(parentDir+"/"+dt);
	}
	
	public String[] getDt() throws IOException
	{	
		FileSystem fs = FileSystem.get(conf);
		
		FileStatus[] subDirArr = fs.listStatus(new Path(parentDir));
		
		dt="dt=";
		lastDt="dt=";
		
		for(FileStatus subDir:subDirArr)
		{
			String name=subDir.getPath().getName();
			if(name.compareTo(dt)>0)
			{
				dt=name;
			}
			if(name.compareTo(lastDt)>0 && name.compareTo(dt)<0)
			{
				lastDt=name;
			}
		}
		
		return new String[]{lastDt,dt};
		
	}
	
	public void run() throws IOException, ClassNotFoundException, InterruptedException, SQLException
	{
		getDt();
		System.out.println(getSrc());
		conf.set("dt", dt.replace("dt=", ""));
		conf.set("mapreduce.input.fileinputformat.split.minsize", Integer.toString(32*1024*1024));
		conf.set("mapreduce.input.fileinputformat.split.minsize", Integer.toString(32*1024*1024));
		Job job=Job.getInstance(conf);
		job.setJobName("更新风险点");
		job.setJarByClass(UpdateRiskModelJob.class);
		job.setMapperClass(UpdateRiskModelMapper.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job,getSrc());
		job.setOutputFormatClass(NullOutputFormat.class);
		int jobStatus = job.waitForCompletion(true)?0:1;
		WriteJobStatus.writeJobStatus(job.getJobName(), dt, jobStatus, job.getCounters().toString());		
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, SQLException
	{
		new UpdateRiskModelJob().run();
	}
}
