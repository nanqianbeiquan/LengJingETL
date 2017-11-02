package etl;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import tools.TableFactory;

public class HiveToHbaseMapper extends Mapper<LongWritable , Text, NullWritable, NullWritable>{

	public Log LOG=LogFactory.getLog(HiveToHbaseMapper.class);
	public HTable dstTable = null;
	public String dstTableName=null;
	public String srcTableName=null;
	public byte[] CF=null;
	public String srcTableID=null;
	public String delimiter=null;
	public int colNums;
	public String colStr=null;
	public byte[][] qualifierArr=null;
	public int companyColNameIdx;
	public int idColIdx;
	public Pattern pattern = Pattern.compile("[\\s`~!@#$%^&*+=|{}':;\",\\[\\].<>/?~！￥…—【】‘；：”“’。，、？]");
	
//	public void setConfig()
//	{	
//		if(srcTableName.equals("judgidentifier"))
//		{
//			srcTableID="20";
//			delimiter="\001";
//			dstTableName="LengJingSF";
//			colStr="id,companyid,title,courtname,casetype,docket,receivetime,receivefrom,receiveurl,litigationtype,"
//					+ "trialclass,instrumenttype,causeaction,amountcount,relatedperson,relatedamount,judgmentresult,content,loser,"
//					+ "personnamelist,companyname,idd,lastmodifytime,companycode,judgmenttime,judgmentid,suittype,f_name";
//			companyNameIdx=20;
//			idIdx=25;
//		}
//		else if(srcTableName.equals("law_shixin"))
//		{
//			srcTableID="17";
//			delimiter="\001";
//			dstTableName="LengJingSF";
//			colStr="no,mc,sj,ah,xqck,xq_mc,xb,nl,dm,fddbr,zxfy,sf,zxyjwh,lasj,ah2,zxyjdw,"
//					+ "wsqdyw,lxqk,ylx,wlv,sxjtqk,fbsj,lastupdatetime,shixinid";
//			companyNameIdx=1;
//			idIdx=23;
//		}
//		else if(srcTableName.equals(""))
//		else if(srcTableName.equals("fact_bltin"))
//		{
//			srcTableID="19";
//			delimiter="\001";
//			dstTableName="LengJingSF";
//			colStr="no,mc,sj,ah,xqck,xq_mc,xb,nl,dm,fddbr,zxfy,sf,zxyjwh,lasj,ah2,zxyjdw,"
//					+ "wsqdyw,lxqk,ylx,wlv,sxjtqk,fbsj,lastupdatetime,shixinid";
//			companyNameIdx=1;
//			idIdx=23;
//		}
//	}
	
	public void setup(Context context) throws IOException, InterruptedException
	{	
		super.setup(context);
		Configuration conf = context.getConfiguration();
		companyColNameIdx=Integer.valueOf(conf.get("companyNameColIdx"));
		idColIdx=Integer.valueOf(conf.get("idColIdx"));
		delimiter=conf.get("delimiter");
		delimiter=delimiter.equals("default")?"\001":delimiter;
		srcTableID=conf.get("tableId");
		dstTableName=conf.get("dstTableName");
		srcTableName=conf.get("srcTableName");
		dstTable=(HTable) TableFactory.getTable(dstTableName);
		CF=Bytes.toBytes(conf.get("CF"));
		dstTable.setAutoFlushTo(false);
		dstTable.setWriteBufferSize(12*1024*1024);
		String[] colArr=conf.get("cols").split(",",-1);
		colNums=colArr.length;
		qualifierArr=new byte[colArr.length][];
		for(int i=0;i<colArr.length;i++)
		{
			qualifierArr[i]=Bytes.toBytes(colArr[i]);
		}
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String vals[]=value.toString().split(delimiter,-1);
		if(vals.length==colNums && !vals[companyColNameIdx].toLowerCase().equals("null") && !vals[companyColNameIdx].equals("\\N"))
		{
			String companyName=vals[companyColNameIdx].trim();
			// 司法信息公司数据包含加密信息，过滤掉包含加密信息的公司名
			if(srcTableName.equals("judgidentifier") && (companyName.contains("**") || companyName.contains("&times;&times;") || companyName.contains("xx")))
			{
				return;
			}
			companyName=pattern.matcher(companyName).replaceAll("");
			String id="";
			if(idColIdx!=-1)
			{
				id=vals[idColIdx];
			}
			byte[] rowKey=Bytes.toBytes(companyName+"_"+srcTableID+"_"+id);
			Put put=new Put(rowKey);
			for(int i=0;i<colNums;i++)
			{
				String val=vals[i];
				if(!val.equals("\\N") && !val.toLowerCase().equals("null"))
				{
					put.addColumn(CF, qualifierArr[i], Bytes.toBytes(val));
				}
			}
			dstTable.put(put);
			
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException 
	{
		super.cleanup(context);
		dstTable.flushCommits();
		dstTable.close();
	}
}
