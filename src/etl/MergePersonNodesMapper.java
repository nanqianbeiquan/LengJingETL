package etl;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.alibaba.fastjson.JSONObject;

import kafka.KafkaAPI;
import tools.TableFactory;

public class MergePersonNodesMapper  extends Mapper<LongWritable , Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String vals[]=StringUtils.splitPreserveAllTokens(value.toString(),"\001");
		String companyName1=vals[0].equals("\\N")?"null":vals[0];
		String companyName2=vals[1].equals("\\N")?"null":vals[1];
		String personName=vals[2].equals("\\N")?"null":vals[2];
		context.write(new Text(personName),new Text(companyName1+"\001"+companyName2));
	}
	
	
}
