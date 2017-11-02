package neo4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import tools.MSSQLClient;

public class UpdateNeo4jFromMSSQL {

	ArrayList<String> cypherList=new ArrayList<String>();
	HashSet<String> tempSet=new HashSet<String>();
	MSSQLClient mssql;
	String curMc=null;
	public UpdateNeo4jFromMSSQL() throws ClassNotFoundException, SQLException
	{
		mssql=new MSSQLClient(
				String.format("jdbc:sqlserver://172.16.0.26:1433;DatabaseName=pachong"),
				"likai", //user
				"d!{<kN5K38u-", //pwd
				false //autoCommit
				);
	}
	
	public String updateBase(String zch) throws Exception
	{
		String sql="SELECT RegistrationNo zch,EnterpriseName mc,LegalRepresentative fddbr,EstablishmentDate clsj,"
				+ "RegisteredCapital zczb,RegistrationStatus djzt,operator jyz,investor tzr,principal fzr "
				+ "FROM Registered_Info where RegistrationNo='"+zch+"'";
		
		ResultSet res = mssql.executeQuery(sql);
		if(res.next())
		{
			curMc=res.getString("mc");
			String line=String.format("%s|%s|%s|%s|%s|%s|%s|%s|%s", 
					res.getString("mc"),
					res.getString("zch"),
					res.getString("clsj"),
					res.getString("zczb"),
					res.getString("djzt"),
					res.getString("fddbr"),
					res.getString("jyz"),
					res.getString("tzr"),
					res.getString("fzr")
					);
			return UpdateNeo4jBak.updateBase(line);
		}
		return null;
		
	}

	public String updateBaxx(String zch) throws Exception
	{
		String sql="SELECT RegistrationNo zch,KeyPerson_Name xm,KeyPerson_Position zw FROM KeyPerson_Info where RegistrationNo='"+zch+"'";
		ResultSet res = mssql.executeQuery(sql);
		String mc=curMc;
		List<String> lines=new ArrayList<String>();
		
		while(res.next())
		{
			String xm=res.getString("xm");
			String zw=res.getString("zw");
			lines.add(xm+"|"+zw);
		}
		return UpdateNeo4jBak.updateKeyPerson(mc, lines);
	}
	
	public String updateGd(String zch) throws Exception
	{
		String sql="SELECT RegistrationNo zch,Shareholder_Type gdlx,Shareholder_Name gdmc,"
				+ "Shareholder_CertificationType zjlx,subscripted_capital tze "
				+ "FROM pachong.dbo.Shareholder_Info where RegistrationNo='"+zch+"'";	
		ResultSet res = mssql.executeQuery(sql);
		List<String> lines=new ArrayList<String>();
		String mc=curMc;
		while(res.next())
		{
			String gdlx=res.getString("gdlx");
			String gdmc=res.getString("gdmc");
			String zjlx=res.getString("zjlx");
			String tze=res.getString("tze");
			lines.add(gdmc+"|"+gdlx+"|"+zjlx+"|"+tze+"|");
			
		}
		return UpdateNeo4jBak.updateShareholder(mc, lines);
	}
	
	public void updateZch(String zch) throws Exception
	{
		System.out.println(updateBase(zch));
		System.out.println(updateBaxx(zch));
		System.out.println(updateGd(zch));
	}
	
	public void updateFromFile(String path) throws Exception
	{
		File src=new File(path);
		InputStreamReader read = new InputStreamReader(new FileInputStream(src));
        BufferedReader bufferedReader = new BufferedReader(read);
        String lineText = null;
        while((lineText = bufferedReader.readLine()) != null)
        {
        	System.out.println(lineText);
        	updateZch(lineText);
//        	break;
        }
        bufferedReader.close();
	}
	
	public static void main(String[] args) throws Exception
	{
		UpdateNeo4jFromMSSQL job=new UpdateNeo4jFromMSSQL();
//		job.updateZch("440101400062919");
//		job.updateZch("91110107MA004UXR1F");
//		job.updateZch("91110107MA0024XE3L");
//		job.updateZch("9131010574615564XK");
//		job.updateZch("430900400001945");
		job.updateFromFile("data/zch_src.txt");
	}
	
}
