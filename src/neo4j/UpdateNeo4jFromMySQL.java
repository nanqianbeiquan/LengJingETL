package neo4j;

import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import tools.MySQL;

public class UpdateNeo4jFromMySQL {

	ArrayList<String> cypherList=new ArrayList<String>();
	HashSet<String> tempSet=new HashSet<String>();
	String curMc=null;
	SimpleDateFormat sdf1=new SimpleDateFormat("yyyy-MM-dd");
	UpdateNeo4j updateJob=new UpdateNeo4j();

	
	public String formatDt(String dt)
	{
		try
		{
			return sdf1.format(sdf1.parse(dt));
		}
		catch (Exception e)
		{
			return "";
		}
	}
	
	public String updateBase() throws Exception
	{
		int i=0;
		String sql="SELECT COALESCE(xydm,zch) zch,replace(replace(replace(replace(mc,')','）'),'(','（'),'\"',''),\"'\",'') mc,clrq clsj,concat(zczb,zcbz) zczb, djzt,COALESCE(fddbr,jyz, tzr, fzr ,zxswhhr) fddbr "
				+ "FROM t_pl_public_org_base ";
		
		ResultSet res = MySQL.executeQuery(sql);
		System.out.println("执行成功！");
		while(res.next())
		{
			
			curMc=res.getString("mc");
			String line=String.format("%s|%s|%s|%s|%s|%s|||", 
					res.getString("mc"),
					res.getString("zch"),
					formatDt(res.getString("clsj")),
					res.getString("zczb"),
					res.getString("djzt"),
					res.getString("fddbr")
					);
			updateJob.updateBase(line);
			if((++i)%1000==0)
			{
				System.out.println("--"+i);
				updateJob.commit();
			}
		}
		updateJob.commit();
		return null;
		
	}

	public String updateBaxx() throws Exception
	{
		String sql="SELECT replace(replace(replace(replace(mc,')','）'),'(','（'),'\"',''),\"'\",'') mc,a.xm,replace(a.zw,'\\\\','/') zw FROM t_pl_public_org_staff a "
				+ "join t_pl_public_org_base b "
				+ "on a.id=b.id "
				+ "order by b.mc";
		System.out.println(sql);
		ResultSet res = MySQL.executeQuery(sql);
		System.out.println("执行成功！");
		String lastMc=null;
		List<String> lines=new ArrayList<String>();
		int i=0;
		while(res.next())
		{
			i++;
			if(i<161000) continue;
			String mc=res.getString("mc");
			if(!mc.equals(lastMc) && lines.size()>0)
			{
				
				updateJob.updateKeyPerson(lastMc, lines);
				lines.clear();
				if((i)%1000==0)
				{
					System.out.println("--"+i);
					updateJob.commit();
				}
			}
			String xm=res.getString("xm");
			String zw=res.getString("zw");
			lastMc=mc;
			lines.add(xm+"|"+zw);
		}
		updateJob.updateKeyPerson(lastMc, lines);
		lines.clear();
		updateJob.commit();
		return null;
	}
	
	public String updateGd() throws Exception
	{
		String sql="SELECT replace(replace(replace(replace(mc,')','）'),'(','（'),'\"',''),\"'\",'') mc,gdlx,replace(replace(replace(gd,')','）'),'(','（'),'\"','') gd,zjlx,sje tze FROM t_pl_public_org_shareholder a "
				+ "join t_pl_public_org_base b "
				+ "on a.id=b.id "
				+ "where gd!='中华人民共和国居民身份证'"
				+ "order by b.mc ";	
		ResultSet res = MySQL.executeQuery(sql);
		System.out.println("执行成功！");
		List<String> lines=new ArrayList<String>();
		String lastMc=null;
		int i=0;
		while(res.next())
		{
			String mc=res.getString("mc");
			if(!mc.equals(lastMc) && lines.size()>0)
			{
				updateJob.updateShareholder(lastMc, lines);
				lines.clear();
			}
			String gdlx=res.getString("gdlx");
			String gdmc=res.getString("gd");
			String zjlx=res.getString("zjlx");
			String tze=res.getString("tze");
			lastMc=mc;
			lines.add(gdmc+"|"+gdlx+"|"+zjlx+"|"+tze+"|");
			if((++i)%1000==0)
			{
//				System.out.println(updateJob.cypherPoolMap.size());
				System.out.println("--"+i);
				updateJob.commit();
//				if(i<=257100)
//				{
//					updateJob.cypherPoolMap.clear();
//				}
//				else
//				{
//					updateJob.commit();
//				}
			}
		}
		System.out.println(lines.size());
		updateJob.updateShareholder(lastMc, lines);
		System.out.println(updateJob.cypherPoolMap);
		lines.clear();
		updateJob.commit();
		return null;
	}

	public void updateFromFile() throws Exception
	{
//		System.out.println(updateBase());
		System.out.println(updateBaxx());
		System.out.println(updateGd());
	}
	
	public static void main(String[] args) throws Exception
	{
		UpdateNeo4jFromMySQL job=new UpdateNeo4jFromMySQL();
		job.updateFromFile();
	}
}
