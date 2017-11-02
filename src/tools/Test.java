package tools;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.fastjson.JSONArray;

import net.sf.json.JSONObject;

public class Test {

	public void test() throws IOException, ClassNotFoundException, SQLException
	{
		FileSystem fs = FileSystem.get(new Configuration());
		System.out.println(fs.getStatus());
	}
	
	public static void test2(String key) throws IOException
	{
		System.setProperty("hadoop.home.dir", "E:\\EclipseProjects\\CDH5.6");
		HTable table=TableFactory.getTable("LengJingSF");
		Get get=new Get(Bytes.toBytes(key));
		
		Result res = table.get(get);
		List<Cell> cells = res.listCells();
		for(Cell c:cells)
		{
			String CF=Bytes.toString(c.getFamilyArray(), c.getFamilyOffset(), c.getFamilyLength());
			String qualifier=Bytes.toString(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength());
			String val=Bytes.toString(c.getValueArray(), c.getValueOffset(), c.getValueLength());
			
			System.out.println(qualifier+" : "+val);
		}
		table.close();
	}
	
	public static void test1() throws IOException
	{
		System.setProperty("hadoop.home.dir", "E:\\EclipseProjects\\CDH5.6");
		HTable table=TableFactory.getTable("LengJingSF");
		HashMap<String,String> cfToTableID=new HashMap<String,String>();
		cfToTableID.put("judgidentifier","20");
		cfToTableID.put("bltin","19");
		cfToTableID.put("law_shixin","17");
		String companyName="����������ӡȾ���޹�˾";
		String columns="judgidentifier:amountcount=500&,judgidentifier:companyname,judgidentifier:courtname,bltin:id";
		String[] colArr=columns.split(",",-1);
		HashMap<String,String> conditionMap=new HashMap<String,String>();
		Scan scan=new Scan();
		String minId="99";
		String maxId="00";
		for(String col:colArr)
		{
			int idx1=col.indexOf(":");
		 	int idx2=col.indexOf("=");
		 	String cf=col.substring(0,idx1);
		 	String qualifier=null;
		 	String condition=null;
		 	if(idx2==-1)
		 	{
		 		qualifier=col.substring(idx1+1);
		 	}
		 	else
		 	{
		 		qualifier=col.substring(idx1+1,idx2);
		 		condition=col.substring(idx2+1);
		 		conditionMap.put(cf+":"+qualifier, condition);
		 	}
		 	
		 	scan.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier));
		 	
		 	String startId=cfToTableID.get(cf);
		 	String stopId=String.format("%02d", Integer.valueOf(startId)+1);
		 	if(startId.compareTo(minId)<0)
		 	{
		 		minId=startId;
		 	}
		 	if(stopId.compareTo(maxId)>0)
		 	{
		 		maxId=stopId;
		 	}
		 }
		 
		 String startRow=companyName+"_"+minId;
		 String stopRow=companyName+"_"+maxId;
		 System.out.println(startRow);
		 System.out.println(stopRow);
		 scan.setStartRow(Bytes.toBytes(startRow));
		 scan.setStopRow(Bytes.toBytes(stopRow));
		 
		 ResultScanner scanRes = table.getScanner(scan);
		 
		 List<JSONObject> listjson =new ArrayList<JSONObject>();
		 
		 for(Result result=scanRes.next();result!=null;result=scanRes.next())
		 {
			 boolean addToRes=true;
			 JSONObject json=new JSONObject();
			 
			 String row=Bytes.toString(result.getRow());
			 System.out.println("--------------------------------------"+row);
			 List<Cell> cellList = result.listCells();
			 for(Cell cell:cellList)
			 {
				 String cf=Bytes.toString(cell.getFamily());
				 String qualifier=Bytes.toString(cell.getQualifier());
				 String val=Bytes.toString(cell.getValue());
				 System.out.println("cf:"+cf+",qualifier:"+qualifier+",val:"+val);
				 json.put(qualifier, val);
				 
				 
				 String condition=conditionMap.get(cf+":"+qualifier);
				 if(condition!=null)
				 {
					 String[] startAndStop=condition.split("&",-1);
					 String startVal=startAndStop[0];
					 String stopVal=startAndStop[1];
					 
					 if(!startVal.equals("") && Long.valueOf(startVal)>Long.valueOf(val))
					 {
						 addToRes=false;
						 break;
					 }
					 if(!stopVal.equals("") && Long.valueOf(stopVal)<Long.valueOf(val))
					 {
						 addToRes=false;
						 break;
					 }
				 }
				 
			 }
			 if(addToRes)
			 {
				 listjson.add(json);
			 }
		 }
		 System.out.println("\n\n"+listjson);

	}
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException, ParseException
	{
		JSONArray arr=new JSONArray();
		arr.add("1");
		arr.add("3");
		arr.add("30");
		arr.remove("3");
		System.out.println(arr);
	}
}
