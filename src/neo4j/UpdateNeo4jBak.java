package neo4j;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import tools.ShareholderParser;
import tools.SplitMoney;


public class UpdateNeo4jBak {

	static SimpleDateFormat sdf2=new SimpleDateFormat("yyyy年M月d日");
	static SimpleDateFormat sdf3=new SimpleDateFormat("yyyy-MM-dd");
	static SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//	Log log=LogFactory.getLog(AddShareholderToGraphReducer.class);
	
	static HashMap<String,String> nameMap=new HashMap<String,String>();
	{
		nameMap.put("Company", "公司名称");
		nameMap.put("Person", "姓名");
		nameMap.put("Unknown", "名称");
	}
	
	public static String parseDate(String dateInfo)
	{
		String res=null;
		try 
		{
			res=sdf3.format(sdf3.parse(dateInfo));
		} 
		catch (ParseException e1) {}
		if(res==null)
		{
			try 
			{
				res=sdf3.format(sdf2.parse(dateInfo));
			}
			catch (ParseException e1) {}
		}
		return res;
	}
	
	/**
	 * 
	 * @param baseLine 输入行，字段间用竖线（“|”）分隔，字段顺序为
	 * 企业名称|注册号|成立时间|注册资本|登记状态|法定代表人|经营者|投资人|负责人
	 * @throws Exception 
	 */
	public static String updateBase(String baseLine) throws Exception
	{
		String[] vals=baseLine.split("\\|",-1);
		String mc=vals[0];
		String zch=vals[1];
		String clsj=parseDate(vals[2]);
		String zczb=vals[3];
		if(!zczb.equals("null") && !zczb.equals(""))
		{
			String[] money=SplitMoney.evaluate(zczb);
			zczb=money[0];
		}
		String djzt=vals[4];
		String fddbr=vals[5];
		String jyz=vals[6];
		String tzr=vals[7];
		String fzr=vals[8];

		String relationShip=null;
		String xm=null;
		if(!fddbr.equals("") && !fddbr.equals("null"))
		{
			relationShip="法定代表人";
			xm=fddbr;
		}
		if(!jyz.equals("") && !jyz.equals("null"))
		{
			relationShip="经营者";
			xm=jyz;
		}
		if(!tzr.equals("") && !tzr.equals("null"))
		{
			relationShip="投资人";
			xm=tzr;
		}
		if(!fzr.equals("") && !fzr.equals("null"))
		{
			relationShip="负责人";
			xm=fzr;
		}
		
		String updateCmd=null;
		String ts=sdf.format(new Date());
		if(xm!=null)
		{
			updateCmd=String.format("merge (c:Company{公司名称:'%s'}) set c.注册号='%s',c.成立时间='%s',c.注册资本='%s',c.登记状态='%s',c.ts='%s' "
					+ "merge (p1:Person{姓名:'%s',关联公司名:'%s'}) set p1.ts='%s' "
					+ "merge (p1)-[r1:%s]->(c) set r1.ts='%s' "
					+ "with c,r1 "
					+ "match ()-[r2:法定代表人|负责人|投资人|经营者]->(c) "
					+ "where id(r1)<>id(r2) delete r2", 
					mc,zch,clsj,zczb,djzt,ts,
					xm,mc,ts,
					relationShip,ts);
		}
		else
		{
			updateCmd=String.format("merge (c:Company{公司名称:'%s'}) set c.注册号='%s',c.成立时间='%s',c.注册资本='%s',c.登记状态='%s',c.ts='%s' "
					+ "with c "
					+ "match ()-[r:法定代表人|负责人|投资人|经营者]->(c) delete r",
					mc,zch,clsj,zczb,djzt,ts);
		}
//		System.out.println(updateCmd);
		return Neo4j.execute(updateCmd,"graph");
	}
	
	/**
	 * 
	 * @param mc 公司名称
	 * @param lines 关系行列表，关系行格式：姓名|职务
	 * @throws Exception 
	 */
	public static String updateKeyPerson(String mc,List<String> lines) throws Exception
	{
		int i=0;
		String ts=sdf.format(new Date());
		String updateCmd=String.format("match (c:Company{公司名称:'%s'}) "
//				+ "with c "
				, mc);
		StringBuilder relationshipIdArr=new StringBuilder();
		for(String line:lines)
		{
			i++;
			String[] vals=line.split("\\|",-1);
			String xm=vals[0];
			String zw=vals[1];
			
			updateCmd+=String.format("merge (p%d:Person{姓名:'%s',关联公司名:'%s'}) set p%d.ts='%s' "
					+ "merge (p%d)-[r%d:任职{职务:'%s'}]->(c) set r%d.ts='%s' " 
					,i,xm,mc,i,ts
					,i,i,zw,i,ts);
			relationshipIdArr.append("id(r"+i+"),");
		}
		if(relationshipIdArr.length()>0)
		{
			relationshipIdArr.setLength(relationshipIdArr.length()-1);
		}
		updateCmd+=String.format("with [%s] as relationshipIdArr,c "
				+ "match ()-[r:任职]->(c) "
				+ "where not(id(r) in relationshipIdArr) "
				+ "delete r", relationshipIdArr);
//		System.out.println(updateCmd);
		return Neo4j.execute(updateCmd,"graph");
	}

	/**
	 * 
	 * @param mc 公司名称
	 * @param lines 关系行列表，关系行格式：股东名称|股东类型|证件类型|投资额|投资占比
	 * @throws Exception 
	 */
	public static String updateShareholder(String mc,List<String> lines) throws Exception
	{
		
		String updateCmd=String.format("match (c:Company{公司名称:'%s'}) ", mc);
		int i=0;
		StringBuilder relationshipIdArr=new StringBuilder();
		double[] sjeArr=new double[lines.size()];
		double totalSje=0;
		String ts=sdf.format(new Date());
		
		boolean existsTzzb=false;
		
		for(String line:lines)
		{
			String[] vals=line.split("\\|",-1);
			String gdmc=vals[0];
			String gdlx=vals[1];
			String zjlx=vals[2];
			String tze=vals[3];
			String tzzb=vals[4];
			String[] money=SplitMoney.evaluate(tze);
			tze=money[0];
			if(!tze.equals("") && !tze.equals("null"))
			{
				sjeArr[i]=Double.valueOf(tze);
			}
			else
			{
				sjeArr[i]=0;
			}
			totalSje+=sjeArr[i];
			
			String shareholderType=ShareholderParser.getShareHolderType(gdlx, zjlx,gdmc);
			if(shareholderType.equals("Person"))
			{
				updateCmd+=String.format("merge (gd%d:Person{姓名:'%s',关联公司名:'%s'}) set gd%d.ts='%s' ",
						i,gdmc,mc,i,ts);
			}
			else if(shareholderType.equals("Company"))
			{
				updateCmd+=String.format("merge (gd%d:Company{公司名称:'%s'}) set gd%d.ts='%s' ",
						i,gdmc,i,ts);
			}
			else
			{
				updateCmd+=String.format("merge (gd%d:Unknown{名称:'%s'}) set gd%d.ts='%s' ",
						i,gdmc,i,ts);
			}
			
			updateCmd+=String.format("merge (gd%d)-[r%d:投资]->(c) set r%d.股东类型='%s',r%d.实缴='%s',r%d.ts='%s'",
					i,i,i,gdlx,i,tze,i,ts);
			
			if(!tzzb.equals(""))
			{
				existsTzzb=true;
				updateCmd+=String.format(",r%d.投资占比='%s' ", i,tzzb);
			}
			else
			{
				updateCmd+=" _tzzb_"+i+"_ ";
			}
//			updateCmd+=String.format("merge (gd%d)-[r%d:投资]->(c) set r%d.股东类型='%s',r%d.实缴='%s',r%d.ts='%s' _tzzb_%d_ ",
//					i,i,i,gdlx,i,tze,i,ts,i);
			relationshipIdArr.append("id(r"+i+"),");
			i++;
		}
		if(relationshipIdArr.length()>0)
		{
			relationshipIdArr.setLength(relationshipIdArr.length()-1);
		}
		if(totalSje==0 || existsTzzb)
		{
			for(int j=0;j<sjeArr.length;j++)
			{
				updateCmd=updateCmd.replace("_tzzb_"+j+"_", "");
			}
		}
		else
		{
			for(int j=0;j<sjeArr.length;j++)
			{
				double tzzb=sjeArr[j]/totalSje;
				updateCmd=updateCmd.replace("_tzzb_"+j+"_", String.format(",r%d.投资占比='%.2f'",j,tzzb));
			}
		}

		updateCmd+=String.format("with [%s] as relationshipIdArr,c "
				+ "match ()-[r:投资]->(c) "
				+ "where not(id(r) in relationshipIdArr) "
				+ "delete r", relationshipIdArr);
//		System.out.println(updateCmd);
		return Neo4j.execute(updateCmd,"graph");
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException
	{
//		updateBase("北京中电财智科技发展有限公司|91110107MA0032PX8T|2016年01月11日|1000 万元|在营（开业）企业|靳哲|||");
//		updateBase("青海华伟房地产开发有限公司|630000110002977|2010-05-20|3000.000000|在营（开业）|周竞雄|||");
//		updateKeyPerson("北京中电财智科技发展有限公司",Arrays.asList(new String[]{"靳哲|执行董事","李光平|经理","黄海美|监事"}));
//		updateShareholder("北京中电财智科技发展有限公司",
//				Arrays.asList(new String[]{"北京中泽万联科技有限公司|法人股东||1|",
//							"靳哲|自然人股东||未公开|",
//							"李光平|自然人股东||2万元|",
//							"黄海美|自然人股东||2|"}));
	}
	
}
