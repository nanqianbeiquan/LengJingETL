package kafka;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import etl.MergePersonNodesMapper;
import etl.MergePersonNodesReducer;

public class AddComanyNameToKafkaJob {

	public int run() throws IOException, ClassNotFoundException, InterruptedException
	{
		int jobStatus=0;
		Configuration conf=HBaseConfiguration.create();
		Job job=Job.getInstance(conf);
		job.setJobName("工商批量更新至kafka");
		job.setJarByClass(AddComanyNameToKafkaJob.class);
		job.setMapperClass(AddComanyNameToKafkaMapper.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job,new Path("/user/hive/warehouse/dw.db/update_company"));
		job.setOutputFormatClass(NullOutputFormat.class);
	    jobStatus=job.waitForCompletion(true)?0:1;
	    return jobStatus;
	}
	
	public static void main(String[] args)
	{
		
	}
	
}
