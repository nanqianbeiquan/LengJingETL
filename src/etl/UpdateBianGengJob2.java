package etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import tools.JobConfig;

public class UpdateBianGengJob2 {

	public int run() throws Exception
	{
		int res=0;
//		String srcTableName="LengJingGSTemp";
//		String dstTableName="LengJingGSTemp";
		String srcTableName="GS";
		String dstTableName="GS";
		Configuration conf=new Configuration();
		conf.set("dstTableName", dstTableName);
		Job job=Job.getInstance(conf);
		job.setJobName("UpdateBianGeng："+srcTableName);
		job.setJarByClass(UpdateBianGengJob2.class);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("Changed_Announcement"));
		scan.addFamily(Bytes.toBytes("Equity_Pledge"));
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
//		scan.setStartRow(Bytes.toBytes("浙江维龙家居用品有限公司_05"));
//		scan.setStopRow(Bytes.toBytes("浙江维龙家居用品有限公司_06"));
		TableMapReduceUtil.initTableMapperJob(
		srcTableName,        // input HBase table name
		  scan,             // Scan instance to control CF and attribute selection
		  UpdateBianGengMapper2.class,   // mapper
		Text.class,             // mapper output key
		Text.class,             // mapper output value
		job);
		job.setNumReduceTasks(30);
		TableMapReduceUtil.initTableReducerJob(srcTableName, UpdateBianGengReducer2.class, job);
		res=job.waitForCompletion(true)?0:1;
		return res;
	}
}
