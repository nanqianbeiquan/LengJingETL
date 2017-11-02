package neo4j;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UpdateRiskModelMapper extends Mapper<LongWritable , Text, NullWritable, NullWritable>{
	
	public Log logger=LogFactory.getLog(UpdateRiskModelMapper.class);
	
	String dt=null;
	
	ArrayList<String> wuList=new ArrayList<String>();
	ArrayList<String> diList=new ArrayList<String>();
	ArrayList<String> zhongList=new ArrayList<String>();
	ArrayList<String> gaoList=new ArrayList<String>();
	int cnt=0;
	int batch=500;
	public void setup(Context context) throws IOException, InterruptedException
	{
		super.setup(context);
		dt=context.getConfiguration().get("dt");
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String vals[]=StringUtils.splitPreserveAllTokens(value.toString(),"\001");
		
		String company=vals[1].equals("\\N")?"null":vals[1];
		int riskRate=Integer.valueOf(vals[2].equals("\\N")?"-1":vals[2]);
//		String riskvalue=vals[5].equals("\\N")?"null":vals[5];
		String riskLevel=null;
		cnt++;
		if(riskRate==0)
		{
			riskLevel="无风险";
			wuList.add(company);
		}
		else if(riskRate==1)
		{
			riskLevel="低风险";
			diList.add(company);
		}
		else if(riskRate==2)
		{
			riskLevel="中风险";
			zhongList.add(company);
		}
		else if(riskRate==3)
		{
			riskLevel="高风险";
			gaoList.add(company);
		}
		else
		{
			return;
		}
		
		if(cnt%batch==0)
		{
			String updateCmd1=String.format("with [%s] as arr "
					+ "match(c:Company) "
					+ "where c.公司名称 in arr "
					+ "and (c.风险评级<>'无风险' or not(has(c.风险评级))) "
					+ "set c.风险评级='无风险',c.风险计算时间='%s' ", 
					getCompanyArr(wuList)
					,dt);
			String updateCmd2=String.format("with [%s] as arr "
					+ "match(c:Company) "
					+ "where c.公司名称 in arr "
					+ "and (c.风险评级<>'低风险' or not(has(c.风险评级))) "
					+ "set c.风险评级='低风险',c.风险计算时间='%s' ", 
					getCompanyArr(diList)
					,dt);
			String updateCmd3=String.format("with [%s] as arr "
					+ "match(c:Company) "
					+ "where c.公司名称 in arr "
					+ "and (c.风险评级<>'中风险' or not(has(c.风险评级))) "
					+ "set c.风险评级='中风险',c.风险计算时间='%s' ", 
					getCompanyArr(zhongList)
					,dt);
			String updateCmd4=String.format("with [%s] as arr "
					+ "match(c:Company) "
					+ "where c.公司名称 in arr "
					+ "and (c.风险评级<>'高风险' or not(has(c.风险评级))) "
					+ "set c.风险评级='高风险',c.风险计算时间='%s' ", 
					getCompanyArr(gaoList)
					,dt);
//			System.out.println(updateCmd1);
//			System.out.println(updateCmd2);
//			System.out.println(updateCmd3);
//			System.out.println(updateCmd4);
//			
			try {
				Neo4j.execute(updateCmd1,"graph");
				Neo4j.execute(updateCmd2,"graph");
				Neo4j.execute(updateCmd3,"graph");
				Neo4j.execute(updateCmd4,"graph");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			wuList.clear();
			diList.clear();
			zhongList.clear();
			gaoList.clear();
		}
		
//		String cypher=String.format("match (c:Company{公司名称:'%s'}) where c.风险评级<>'%s' set c.风险评级='%s',c.风险计算时间='%s'",company,riskLevel,riskLevel,dt);
//		neo4j.executeUpdate(cypher);
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		super.cleanup(context);
		String updateCmd1=String.format("with [%s] as arr "
				+ "match(c:Company) "
				+ "where c.公司名称 in arr "
				+ "and (c.风险评级<>'无风险' or not(has(c.风险评级))) "
				+ "set c.风险评级='无风险',c.风险计算时间='%s' ", 
				getCompanyArr(wuList)
				,dt);
		String updateCmd2=String.format("with [%s] as arr "
				+ "match(c:Company) "
				+ "where c.公司名称 in arr "
				+ "and (c.风险评级<>'低风险' or not(has(c.风险评级))) "
				+ "set c.风险评级='低风险',c.风险计算时间='%s' ", 
				getCompanyArr(diList)
				,dt);
		String updateCmd3=String.format("with [%s] as arr "
				+ "match(c:Company) "
				+ "where c.公司名称 in arr "
				+ "and (c.风险评级<>'中风险' or not(has(c.风险评级))) "
				+ "set c.风险评级='中风险',c.风险计算时间='%s' ", 
				getCompanyArr(zhongList)
				,dt);
		String updateCmd4=String.format("with [%s] as arr "
				+ "match(c:Company) "
				+ "where c.公司名称 in arr "
				+ "and (c.风险评级<>'高风险' or not(has(c.风险评级))) "
				+ "set c.风险评级='高风险',c.风险计算时间='%s' ", 
				getCompanyArr(gaoList)
				,dt);
		
		try {
			Neo4j.execute(updateCmd1,"graph");
			Neo4j.execute(updateCmd2,"graph");
			Neo4j.execute(updateCmd3,"graph");
			Neo4j.execute(updateCmd4,"graph");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		wuList.clear();
		diList.clear();
		zhongList.clear();
		gaoList.clear();
	}
	
	public String getCompanyArr(ArrayList<String> companyList)
	{
		StringBuilder arrBuilder=new StringBuilder();
		for(String company:companyList)
		{
			arrBuilder.append("'"+company+"',");
		}
		if(arrBuilder.length()>0)
		{
			arrBuilder.setLength(arrBuilder.length()-1);
		}
		return arrBuilder.toString();
	}
}
