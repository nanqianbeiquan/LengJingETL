package neo4j;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;

import tools.ShareholderParser;
import tools.SplitMoney;

public class UpdateNeo4j {

	static SimpleDateFormat sdf2=new SimpleDateFormat("yyyy年M月d日");
	static SimpleDateFormat sdf3=new SimpleDateFormat("yyyy-MM-dd");
	static SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static HashMap<String,String> nameMap=new HashMap<String,String>();

	HashMap<String,String> cypherPoolMap= new HashMap<String,String>();
	HashSet<String> updateMcSet=new HashSet<String>();
	
	static
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
	 * @return 
	 * @throws SQLException
	 */
	
	public void updateBase(String baseLine) throws SQLException
	{
		String[] vals=baseLine.split("\\|",-1);
		String mc=vals[0];
		String fddbr=vals[5];
		String jyz=vals[6];
		String tzr=vals[7];
		String fzr=vals[8];
		String relationShip="法定代表人";
		String xm=null;
		
		Neo4j.addToUpdateMcSet(mc);
		
		String ts=sdf.format(new Date());
		if(!fddbr.equals("") && !fddbr.equals("null"))
		{
			xm=fddbr;
		}
		if(!jyz.equals("") && !jyz.equals("null"))
		{
			xm=jyz;
		}
		if(!tzr.equals("") && !tzr.equals("null"))
		{
			xm=tzr;
		}
		if(!fzr.equals("") && !fzr.equals("null"))
		{
			xm=fzr;
		}

		String updateCmd=null;
		
		if(xm!=null)
		{
			String nodeType=ShareholderParser.getShareHolderType("", "",xm);
			String mergeFddbrNodeCmd=null;
			if(nodeType.equals("Company"))
			{
				mergeFddbrNodeCmd=String.format("merge (p1:Company{公司名称:'%s'}) set p1.ts='%s' ", xm,ts);
			}
			else
			{
				mergeFddbrNodeCmd=String.format("merge (p1:Person{姓名:'%s',key:'%s|%s'}) set p1.ts='%s' ", xm,xm,mc,ts);
			}
			updateCmd=String.format("merge (c:Company{公司名称:'%s'}) set c.ts='%s' "
					+ mergeFddbrNodeCmd
					+ "merge (p1)-[r1:%s]->(c) set r1.ts='%s' "
					+ "with c,r1 "
					+ "match ()-[r2:法定代表人]->(c) "
					+ "where id(r1)<>id(r2) delete r2", 
					mc,ts,
					relationShip,ts);
		}
		else
		{
			updateCmd=String.format("merge (c:Company{公司名称:'%s'}) set c.ts='%s' "
					+ "with c "
					+ "match ()-[r:法定代表人]->(c) delete r",
					mc,ts);
		}
//		System.out.println(updateCmd);
//		return Neo4j.execute(updateCmd, "default");
//		Neo4j.addToPool(updateCmd);
		cypherPoolMap.put("base|"+mc, updateCmd);
	}
	
	/**
	 * 
	 * @param mc 公司名称
	 * @param lines 关系行列表，关系行格式：姓名|职务
	 * @return 
	 */
	public void updateKeyPerson(String mc,List<String> lines)
	{
		int i=0;
		String ts=sdf.format(new Date());
		String updateCmd=String.format("merge (c:Company{公司名称:'%s'}) "
//				+ "with c "
				, mc);
		StringBuilder relationshipIdArr=new StringBuilder();
		for(String line:lines)
		{
			i++;
			String[] vals=line.split("\\|",-1);
			String xm=vals[0];
			String zw=vals[1];
			
			updateCmd+=String.format("merge (p%d:Person{姓名:'%s',key:'%s|%s'}) set p%d.ts='%s' "
					+ "merge (p%d)-[r%d:任职{职务:'%s'}]->(c) set r%d.ts='%s' " 
					,i,xm,xm,mc,i,ts
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
//		return Neo4j.execute(updateCmd, "default");
//		Neo4j.addToPool(updateCmd);
		cypherPoolMap.put("keyPerson|"+mc, updateCmd);
	}

	/**
	 * 
	 * @param mc 公司名称
	 * @param lines 关系行列表，关系行格式：股东名称|股东类型|证件类型|投资额|投资占比
	 * @return 
	 * @throws Exception 
	 */
	public void updateShareholder(String mc,List<String> lines) throws Exception
	{
		boolean existsTzzb=false;
		double totalTze=0;
		for(String line:lines)
		{
			String[] vals=line.split("\\|",-1);
			String tze=vals[3];
			String tzzb=vals[4];
			if(!tzzb.equals(""))
			{
				existsTzzb=true;
			}
			double tzed=0;
			try
			{
				String[] money=SplitMoney.evaluate(tze);
				tze=money[0];
				tzed=Double.valueOf(tze);
			}
			catch (Exception e){}
			totalTze+=tzed;
		}
		if(!existsTzzb && totalTze>0)
		{
			for(int k=0;k<lines.size();k++)
			{
				String line=lines.get(k);
				
				String[] vals=line.split("\\|",-1);
				String tze=vals[3];
				double tzed=0;
				try
				{
					String[] money=SplitMoney.evaluate(tze);
					tze=money[0];
					tzed=Double.valueOf(tze);
				}
				catch (Exception e){}
				vals[3]=tze;
				vals[4]=String.valueOf(tzed/totalTze);
				String newLine=StringUtils.join(vals,"|");
				lines.set(k, newLine);
			}
		}
		if(lines.size()>200)
		{
			updateShareholderBigSet(mc,lines);
			return;
		}
		String updateCmd=String.format("merge (c:Company{公司名称:'%s'}) ", mc);
		int i=0;
		StringBuilder relationshipIdArr=new StringBuilder();
		String ts=sdf.format(new Date());
		
		for(String line:lines)
		{
			String[] vals=line.split("\\|",-1);
			String gdmc=vals[0];
			String gdlx=vals[1];
			String zjlx=vals[2];
			String tze=vals[3];
			String tzzb=vals[4];
			
			String shareholderType=ShareholderParser.getShareHolderType(gdlx, zjlx,gdmc);
			if(shareholderType.equals("Person"))
			{
				updateCmd+=String.format("merge (gd%d:Person{姓名:'%s',key:'%s|%s'}) set gd%d.ts='%s' ",
						i,gdmc,gdmc,mc,i,ts);
			}
			else if(shareholderType.equals("Company"))
			{
				updateCmd+=String.format("merge (gd%d:Company{公司名称:'%s'}) set gd%d.ts='%s' ",
						i,gdmc,i,ts);
			}
			else
			{
				updateCmd+=String.format("merge (gd%d:Unknown{名称:'%s',key:'%s|%s'}) set gd%d.ts='%s' ",
						i,gdmc,gdmc,mc,i,ts);
			}
			
			updateCmd+=String.format("merge (gd%d)-[r%d:投资]->(c) set r%d.股东类型='%s',r%d.投资额='%s',r%d.ts='%s' ",
					i,i,i,gdlx,i,tze,i,ts);
			
			if(!tzzb.equals(""))
			{
				updateCmd+=String.format(",r%d.投资占比='%s' ", i,tzzb);
			}
			relationshipIdArr.append("id(r"+i+"),");
			i++;
		}
		if(relationshipIdArr.length()>0)
		{
			relationshipIdArr.setLength(relationshipIdArr.length()-1);
		}
		updateCmd+=String.format("with [%s] as relationshipIdArr,c "
				+ "match ()-[r:投资]->(c) "
				+ "where not(id(r) in relationshipIdArr) "
				+ "delete r", relationshipIdArr);
		cypherPoolMap.put("shareholder|"+mc, updateCmd);
	}
	
	/**
	 * 
	 * @param mc 公司名称
	 * @param lines 关系行列表，关系行格式：股东名称|股东类型|证件类型|投资额|投资占比
	 * @return 
	 * @throws Exception 
	 */
	public void updateShareholderBigSet(String mc,List<String> lines) throws Exception
	{
		int i=0;
		String ts=sdf.format(new Date());
		String deleteCmd=String.format("match (c:Company{公司名称:'%s'})<-[r:投资]-() delete r", mc);
		Neo4j.execute(deleteCmd, "graph");
		int j=0;
		int startIdx=0;
		
		while(true)
		{
			j++;
			int stopIdx=Math.min(startIdx+200,lines.size());
			List<String> subList=lines.subList(startIdx, stopIdx);
			
			String updateCmd=String.format("merge (c:Company{公司名称:'%s'}) ", mc);
			
			for(String line:subList)
			{
				String[] vals=line.split("\\|",-1);
				String gdmc=vals[0];
				String gdlx=vals[1];
				String zjlx=vals[2];
				String tze=vals[3];
				String tzzb=vals[4];
				String[] money=SplitMoney.evaluate(tze);
				tze=money[0];
				
				String shareholderType=ShareholderParser.getShareHolderType(gdlx, zjlx,gdmc);
				if(shareholderType.equals("Person"))
				{
					updateCmd+=String.format("merge (gd%d:Person{姓名:'%s',key:'%s|%s'}) set gd%d.ts='%s' ",
							i,gdmc,gdmc,mc,i,ts);
				}
				else if(shareholderType.equals("Company"))
				{
					updateCmd+=String.format("merge (gd%d:Company{公司名称:'%s'}) set gd%d.ts='%s' ",
							i,gdmc,i,ts);
				}
				else
				{
					updateCmd+=String.format("merge (gd%d:Unknown{名称:'%s',key:'%s|%s'}) set gd%d.ts='%s' ",
							i,gdmc,gdmc,mc,i,ts);
				}
				
				updateCmd+=String.format("merge (gd%d)-[r%d:投资]->(c) set r%d.股东类型='%s',r%d.投资额='%s',r%d.ts='%s' ",
						i,i,i,gdlx,i,tze,i,ts);
				
				if(!tzzb.equals(""))
				{
					updateCmd+=String.format(",r%d.投资占比='%s' ", i,tzzb);
				}
				i++;
			}
			
			cypherPoolMap.put("shareholder|"+mc+"_"+j, updateCmd);
			
			if(stopIdx==lines.size())
			{
				break;
			}
			else
			{
				startIdx=stopIdx;
			}
		}
	}
	
	public void updateInvestment(String mc2,List<String> lines)
	{
		String ts=sdf.format(new Date());
		for(String line:lines)
		{
			String[] vals=line.split("\\|",-1);
			String mc=vals[0];
			String zczb=vals[3];
			if(!zczb.equals("null") && !zczb.equals(""))
			{
				String[] money=SplitMoney.evaluate(zczb);
				zczb=money[0];
			}
			String fddbr=vals[5];
			String tze=vals[6];
			try
			{
				String[] money=SplitMoney.evaluate(tze);
				tze=money[0];
			}
			catch (Exception e)
			{
				tze="";
			}
			String tzzb=vals[7];
			String relationShip=null;
			String xm=null;
			if(!fddbr.equals("") && !fddbr.equals("null"))
			{
				relationShip="法定代表人";
				xm=fddbr;
			}

			String updateCmd=String.format("merge (c2:Company{公司名称:'%s'}) ",mc2);
			if(xm!=null)
			{
				String nodeType=ShareholderParser.getShareHolderType("", "",xm);
				String mergeFddbrNodeCmd=null;
				if(nodeType.equals("Company"))
				{
					mergeFddbrNodeCmd=String.format("merge (p1:Company{公司名称:'%s'}) set p1.ts='%s' ", xm,ts);
				}
				else
				{
					mergeFddbrNodeCmd=String.format("merge (p1:Person{姓名:'%s',key:'%s|%s'}) set p1.ts='%s' ", xm,xm,mc,ts);
				}
				
				updateCmd+=String.format("merge (c:Company{公司名称:'%s'}) set c.ts='%s' "
						+ mergeFddbrNodeCmd
						+ "merge (p1)-[r1:%s]->(c) set r1.ts='%s' "
						+ "with c,r1 "
						+ "match ()-[r2:法定代表人]->(c) "
						+ "where id(r1)<>id(r2) delete r2 ", 
						mc,ts,
						relationShip,ts);
			}
			else
			{
				updateCmd+=String.format("merge (c:Company{公司名称:'%s'}) set c.ts='%s' "
						+ "with c "
						+ "match ()-[r:法定代表人]->(c) delete r ",
						mc,ts);
			}
			cypherPoolMap.put("investment1|"+mc2+"|"+mc, updateCmd);
			String updateCmd2=String.format("merge (c2:Company{公司名称:'%s'}) "
					+ "merge (c1:Company{公司名称:'%s'}) "
					+ "merge (c2)-[r1:投资]->(c1) set r1.投资额='%s',r1.投资占比='%s',r1.ts='%s' "
					+ "with r1,c1 "
					+ "match ()-[r2]->(c1) where id(r2)<>id(r1) remove r2.投资占比 "
					,mc2
					,mc
					,tze,tzzb,ts);
			cypherPoolMap.put("investment2|"+mc2+"|"+mc, updateCmd2);
		}
	}
	
	public void commit() throws Exception
	{
		Iterator<Entry<String, String>> iterator = cypherPoolMap.entrySet().iterator();
		List<String> pool=new ArrayList<String>();
		while(iterator.hasNext())
		{
			Entry<String, String> entry = iterator.next();
			String key=entry.getKey();
			String cypher=entry.getValue();
			if(key.startsWith("investment"))
			{
				String mc=key.split("\\|")[2];
				if(!updateMcSet.contains(mc))
				{
					pool.add(cypher);
				}
				else
				{
					System.out.println(mc+"将进行完整更新，不单独更新此条对外投资关系");
				}
			}
			else
			{
				pool.add(cypher);
			}
		}
		
		int startIdx=0;
//		System.out.println(pool.size());
		while(true)
		{
			int stopIdx=Math.min(startIdx+1000,pool.size());
			List<String> subList=pool.subList(startIdx, stopIdx);
			Neo4j.execute(subList, "graph");
			if(stopIdx==pool.size())
			{
				break;
			}
			else
			{
				startIdx=stopIdx;
			}
		}
		cypherPoolMap.clear();
		updateMcSet.clear();
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException
	{
		
	}
	
}
