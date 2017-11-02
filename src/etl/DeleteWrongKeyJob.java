package etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import tools.JobConfig;

public class DeleteWrongKeyJob {

	public int run(JobConfig jobConf) throws Exception
	{
		int res=0;
		String srcTableName="LengJingGSTemp";
		String dstTableName="LengJingGSTemp";
		Configuration conf=new Configuration();
		conf.set("dstTableName", dstTableName);
		Job job=Job.getInstance(conf);
		job.setJobName("DeleteWrongKeyJob："+srcTableName);
		job.setJarByClass(DeleteWrongKeyJob.class);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("Shareholder_Info"));
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		TableMapReduceUtil.initTableMapperJob(
		srcTableName,        // input HBase table name
		  scan,             // Scan instance to control CF and attribute selection
		  DeleteWrongKeyMapper.class,   // mapper
		null,             // mapper output key
		null,             // mapper output value
		job);
		job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything from mapper
		job.setNumReduceTasks(0);
		res=job.waitForCompletion(true)?0:1;
		return res;
	}
}
