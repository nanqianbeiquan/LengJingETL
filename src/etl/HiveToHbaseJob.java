package etl;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import tools.JobConfig;

public class HiveToHbaseJob {

	public String database=null;
	public String srcTableName=null;
	public String dt=null;
	public Configuration conf=new Configuration();
	
	public Path getSrcPath()
	{
		String src="/user/hive/warehouse/"+database+".db/"+srcTableName;
		if(dt!=null)
		{
			src=src+"/dt="+dt;
		}
		return new Path(src);
	}
	public HiveToHbaseJob(){}
	
	public void loadTableConfig() throws Exception
	{
		String tableId=null;
		String database=null;
		String delimiter=null;
		String cols=null;
		String CF=null;
		String companyNameColIdx=null;
		String idColIdx=null;
		String dstTableName=null;
		File f=new File("TableConfig.xml");
		SAXBuilder builder = new SAXBuilder();
        Document doc = builder.build(f);
        Element root = doc.getRootElement();
        List<Element> tableList = root.getChildren("table");
        for(Element table:tableList)
        {
        	String tableName=table.getChildTextTrim("tableName");
        	if(srcTableName.equals(tableName))
        	{
        		tableId=table.getChildTextTrim("tableId");
        		database=table.getChildTextTrim("database");
        		delimiter=table.getChildTextTrim("delimiter");
        		cols=table.getChildTextTrim("cols");
        		CF=table.getChildTextTrim("CF");
        		companyNameColIdx=table.getChildTextTrim("companyNameColIdx");
        		idColIdx=table.getChildTextTrim("idColIdx");
        		dstTableName=table.getChildTextTrim("dstTableName");
        		break;
        	}
        }
        if(tableId==null)
        {
        	throw new Exception("未知表来源");
        }
        else
        {
        	conf.set("tableId", tableId);
        	conf.set("database", database);
        	conf.set("delimiter", delimiter);
        	conf.set("cols", cols);
        	conf.set("CF", CF);
        	conf.set("companyNameColIdx", companyNameColIdx);
        	conf.set("idColIdx", idColIdx);
        	conf.set("dstTableName",dstTableName);
        }
//        System.out.println(conf.get("tableId"));
//        System.out.println(conf.get("database"));
//        System.out.println(conf.get("delimiter"));
//        System.out.println(conf.get("cols"));
//        System.out.println(conf.get("CF"));
//        System.out.println(conf.get("companyNameColIdx"));
//        System.out.println(conf.get("idColIdx"));
        
	}
	
	public int run(JobConfig jobConf) throws Exception
	{
		int res=0;
		srcTableName=jobConf.getString("srcTableName");
		dt=jobConf.getString("dt");
		conf.set("srcTableName", srcTableName);
		loadTableConfig();
		database=conf.get("database");
		Job job=Job.getInstance(conf);
		job.setJobName("Load "+database+"."+srcTableName+" from hive to hbase");
		job.setJarByClass(HiveToHbaseJob.class);
		job.setMapperClass(HiveToHbaseMapper.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job,getSrcPath());
		job.setOutputFormatClass(NullOutputFormat.class);
		res=job.waitForCompletion(true)?0:1;
		return res;
	}
	
	
	public static void main(String args[]) throws Exception
	{
		HiveToHbaseJob job=new HiveToHbaseJob();
		job.srcTableName="pachong_registered_info";
//		System.out.println(job.getSrcPath());
		job.loadTableConfig();
	}
}
