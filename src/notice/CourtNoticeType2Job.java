package notice;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.commons.codec.digest.DigestUtils;

import tools.ExecShell;
import tools.JobConfig;
import tools.MySQL;
import tools.ParseDate;
import tools.SysFunc;
import tools.TimeUtils;
import tools.WriteJobStatus; 

public class CourtNoticeType2Job {
	
	String startDt=TimeUtils.getYesterday();
	String stopDt=TimeUtils.getToday();
	String tableId="27";
	Pattern pattern = Pattern.compile("申请再审人|原审\\(一审\\)诉讼地位|抗诉机关|诉讼|反诉|被申诉人|申诉人|再审|申请人|原审|被上诉人|上诉人|被告人|原告|被告|第三人|被");
	public String getSelectCmd() throws IOException
	{
		File src=new File("conf/notice_type2.sql");
		FileInputStream reader = new FileInputStream(src);
		int l=(int) src.length();
		byte[] content=new byte[l];
		reader.read(content);
		reader.close();
		String selectCmd=new String(content);
		selectCmd=selectCmd.replace("@start_dt", "'"+startDt+"'");
		selectCmd=selectCmd.replace("@stop_dt", "'"+stopDt+"'");
//		System.out.println(selectCmd.length()-100);
//		System.out.println(selectCmd);
		return selectCmd;
	}
	
	public void run(JobConfig jobConf) throws ClassNotFoundException, SQLException, IOException, ParseException, InterruptedException
	{
		if(jobConf.hasProperty("startDt"))
		{
			startDt=jobConf.getString("startDt");
		}
		if(jobConf.hasProperty("stopDt"))
		{
			stopDt=jobConf.getString("stopDt");
		}
		String dt=TimeUtils.dateAdd(stopDt, -1);
		File f=new File("data/notice_type2_"+startDt+"_"+stopDt+".txt");
		FileWriter fw=new FileWriter(f);
		String path = f.getAbsolutePath();
		String selectCmd=getSelectCmd();
		Pattern pattern1 = Pattern.compile("\\s");
		
		int jobStatus1=0;
		String remark1="";
		try
		{
			ResultSet result = MySQL.executeQuery(selectCmd);
			int cnt=0;
			while(result.next())
			{
				String faYuanMingCheng=pattern1.matcher(result.getString("fa_yuan_ming_cheng")).replaceAll(""); 
				String content=result.getString("content"); 
				String area=result.getString("area");
				String kaiTingRiQi="";
				String anYou="";
				String anHao="";
				String shenLiFaTing="";
				String zhuShenFaGuan="";
				String chengBanTing="";
				String dangShiRen="";
				String md5=null;
				content=content.replace("(", "（").replace(")", "）");
				if(area.equals("北京"))
				{
					
					md5=DigestUtils.md5Hex(content);
					HashMap<String,String> parseResult=BjCourtNotice.parse(content);
					if(parseResult==null)
					{
						continue;
					}
					anYou=parseResult.get("案由");
					shenLiFaTing=parseResult.get("审理法庭");
					dangShiRen=parseResult.get("当事人");
					kaiTingRiQi=parseResult.get("开庭日期");
					kaiTingRiQi=ParseDate.parse(kaiTingRiQi);
//					if(dangShiRen.equals(""))
//					{
//						System.out.println(content);
//						System.out.println(parseResult);
//					}
					
				}
				
				String outLine1=kaiTingRiQi+"\001"+anYou+"\001"+anHao+"\001"
						+faYuanMingCheng+"\001"+shenLiFaTing+"\001"
						+zhuShenFaGuan+"\001"+chengBanTing+"\001"+dangShiRen;
				
				String[] companyArr=dangShiRen.split("[;,；:：，、.与和同]");
				for(String company:companyArr)
				{
					company=pattern.matcher(company).replaceAll("");
					if(company.contains("公司")
							|| company.contains("集团")
							|| company.contains("企业")
							|| company.contains("超市")
							|| company.contains("有限合伙")
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
						
						String key=company+"_"+tableId+"_"+md5;
						String outLine=key+"\001"+company+"\001"+outLine1+"\n";
//						System.out.println(outLine);
						fw.write(outLine);
					}
				}
				if((++cnt)%1000==0)
				{
					System.out.println("++"+cnt);
				}
//				System.out.println("++"+cnt);
			}
			fw.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			remark1=SysFunc.getError(e);
			jobStatus1=1;
		}

		WriteJobStatus.writeJobStatus("开庭公告解析（类型2）", dt, jobStatus1, remark1.replace("'", ""));
		String[] args=new String[]
				{"hive"
				,"-e"
				,String.format("\"LOAD DATA LOCAL INPATH '%s' into TABLE ods.kai_ting_gong_gao partition(dt='%s')\"",path,dt)
				};
		int jobStatus2=0;
		String remark2="";
		try
		{
			remark2=ExecShell.exec(args);
		}
		catch (Exception e)
		{
			remark2=SysFunc.getError(e);
			jobStatus2=1;
		}
		WriteJobStatus.writeJobStatus("开庭公告导入hive（类型2）", dt, jobStatus2, remark2.replace("'", ""));
		String[] load2HbaseArgs=new String[]
				{"sh"
				,"/home/likai/ImportCourtNoticeToHbase.sh"
				,dt
				};
		int jobStatus3=0;
		String remark3="";
		try
		{
			remark3=ExecShell.exec(load2HbaseArgs);
		}
		catch (Exception e)
		{
			remark3=SysFunc.getError(e);
			jobStatus3=1;
		}
		WriteJobStatus.writeJobStatus("开庭公告导入hbase）", dt, jobStatus3, remark3.replace("'", ""));
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, ParseException, InterruptedException
	{
		String[] newArgs={"--startDt=2016-01-01"};
		JobConfig jobConf=new JobConfig(newArgs);
		CourtNoticeType2Job job =new CourtNoticeType2Job();
//		job.getSelectCmd();
		job.run(jobConf);
	}
}
