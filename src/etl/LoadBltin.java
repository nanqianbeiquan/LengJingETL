package etl;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import tools.MySQL;
import tools.TableFactory;

public class LoadBltin {
	
	public int run() throws ClassNotFoundException, SQLException, IOException
	{
		int res=0;
		Pattern pattern1 = Pattern.compile("\\s");
		String colStr = "id,crt_name,rld_prn,pub_date,blt_type,blt_content";
		String[] cols=colStr.split(",",-1);
		int colNums=cols.length;
		HTable dstTable=(HTable) TableFactory.getTable("LengJingSF");
		dstTable.setAutoFlushTo(false);
		dstTable.setWriteBufferSize(12*1024*1024);
		
		byte[] CF=Bytes.toBytes("bltin");
		byte[][] qualifierArr=new byte[cols.length][];
		for(int i=0;i<cols.length;i++)
		{
			qualifierArr[i]=Bytes.toBytes(cols[i]);
		}
		ResultSet result = MySQL.executeQuery("SELECT "+colStr+" FROM bltin where rld_prn is not null and rld_prn!='' and id is not null");
		int cnt=0;
		while(result.next())
		{
			String rldPrn=pattern1.matcher(result.getString("rld_prn")).replaceAll("");
			String id=result.getString("id");
			String[] companyArr=rldPrn.split("[;,；，、.]");
			for(String company:companyArr)
			{
				if(company.contains("公司")
						|| company.contains("集团")
						|| company.contains("企业")
						|| company.contains("超市")
						|| (company.length()>5 && (
								company.endsWith("厂")
								|| company.endsWith("社")
								|| company.endsWith("场")
								|| company.endsWith("店")
								|| company.endsWith("行")
								|| company.endsWith("部")
							))
						)
				{
					byte[] rowKey=Bytes.toBytes(company+"_19_"+id);
					Put put=new Put(rowKey);
					for(int i=0;i<colNums;i++)
					{
						String val=result.getString(i+1);
						if(val!=null)
						{
							put.addColumn(CF, qualifierArr[i], Bytes.toBytes(val));
						}
						
					}
					dstTable.put(put);
				}
				
			}
			if((cnt++)%1000==0)
			{
				System.out.println("++"+cnt);
			}
		}
		dstTable.flushCommits();
		dstTable.close();
		return res;
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException
	{
		LoadBltin job =new LoadBltin();
		job.run();
	}
}
