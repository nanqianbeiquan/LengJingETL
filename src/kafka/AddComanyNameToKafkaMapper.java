package kafka;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.alibaba.fastjson.JSONObject;

public class AddComanyNameToKafkaMapper extends Mapper<LongWritable , Text, NullWritable, NullWritable>{

	KafkaAPI kafka;
	
	public void setup(Context context) throws IOException, InterruptedException
	{	
		super.setup(context);
		kafka=new KafkaAPI();
		kafka.initProducer();
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String vals[]=StringUtils.splitPreserveAllTokens(value.toString(),"\001");
		
		String companyName=vals[0];
		String provinceName=vals[1];
		String provinceCode=vals[2];
		
		JSONObject json=new JSONObject();
		json.put("companyName", companyName);
		json.put("provinceName", provinceName);
		json.put("provinceCode", provinceCode);
		if(provinceCode!=null && !provinceCode.equals("null") && !provinceCode.equals("\\N"))
		{
			if(provinceCode.equals("21"))
			{
				String topic="GsSrc"+provinceCode;
				String message=json.toJSONString();
				kafka.send(topic, message);
			}
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException
	{	
		super.setup(context);
		
	}
	
}
