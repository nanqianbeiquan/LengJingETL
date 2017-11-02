package etl;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
/*
 * 处理的数据源是多个小文件
 * 会把多个小文件合并处理，合并的大小如果小于128M，就当成一个InputSplit处理。
 * 与SequenceFileInputFormat不同的是，SequenceFileInputFormat处理的数据源是合并好的SequencceFile类型的数据。
 */
public class CombineTextInputFormatTest {
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> 
	{	
		Log logger=LogFactory.getLog(MyMapper.class);
		final Text k2 = new Text();
		final LongWritable v2 = new LongWritable();

		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws InterruptedException, IOException {
			final String line = value.toString();
			logger.info("+++"+line);
			
		}
	}

	public void run() throws Exception {
		final Configuration conf = new Configuration();
		final Job job = Job.getInstance(conf, CombineTextInputFormatTest.class.getSimpleName());
		// 1.1
		FileInputFormat.setInputPaths(job,"/crawler/judgment/test");
		
		//这里改了一下
		job.setInputFormatClass(CombineFileInputFormat.class);
		// 1.2
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		// 1.3 默认只有一个分区
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(NullOutputFormat.class);
		// 执行打成jar包的程序时，必须调用下面的方法
		job.setJarByClass(CombineTextInputFormatTest.class);
		job.waitForCompletion(true);
	}
}
