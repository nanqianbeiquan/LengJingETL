package etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import tools.JobConfig;

public class UpdateBianGengJob {

	public int run() throws Exception
	{
		int res=0;
		String srcTableName="LengJingThirdPartInterfaceRecordTemp";
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJobName("zhongshuFlush");
		job.setJarByClass(UpdateBianGengJob.class);
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("LastUpdateTime"), Bytes.toBytes("zhongshu"));
//		scan.addFamily(Bytes.toBytes("LastUpdateTime"));
//		scan.addFamily(Bytes.toBytes("Equity_Pledge"));
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs

		TableMapReduceUtil.initTableMapperJob(
		srcTableName,        // input HBase table name
		  scan,             // Scan instance to control CF and attribute selection
		  UpdateBianGengMapper.class,   // mapper
		Text.class,             // mapper output key
		Text.class,             // mapper output value
		job);
		Path outputDir = new Path("/home/join/output"); 
		job.setNumReduceTasks(0);
		FileOutputFormat.setOutputPath(job, outputDir); 
//		TableMapReduceUtil.initTableReducerJob(srcTableName, UpdateBianGengReducer.class, job);
		res=job.waitForCompletion(true)?0:1;
		return res;
	}
}
