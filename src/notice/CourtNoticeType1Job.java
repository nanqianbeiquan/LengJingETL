package notice;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.regex.Pattern;

import org.apache.commons.codec.digest.DigestUtils;

import tools.ExecShell;
import tools.JobConfig;
import tools.MySQL;
import tools.ParseDate;
import tools.SysFunc;
import tools.TimeUtils;
import tools.WriteJobStatus; 

public class CourtNoticeType1Job {
	
	String startDt=TimeUtils.getYesterday();
	String stopDt=TimeUtils.getToday();
	String tableId="27";
	
	public String getSelectCmd() throws IOException
	{
		File src=new File("conf/notice_type1.sql");
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
		File f=new File("data/notice_type1_"+startDt+"_"+stopDt+".txt");
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
				String kaiTingRiQi=result.getString("kai_ting_ri_qi").trim();
				kaiTingRiQi=ParseDate.parse(kaiTingRiQi);
				String anYou=pattern1.matcher(result.getString("an_you")).replaceAll("");
				String anHao=pattern1.matcher(result.getString("an_hao")).replaceAll("");
				anHao=anHao.replace("(", "（").replace(")", "）");
				String faYuanMingCheng=pattern1.matcher(result.getString("fa_yuan_ming_cheng")).replaceAll("");
				String shenLiFaTing=pattern1.matcher(result.getString("shen_li_fa_ting")).replaceAll("");
				String zhuShenFaGuan=pattern1.matcher(result.getString("zhu_shen_fa_guan")).replaceAll("");
				String chengBanTing=pattern1.matcher(result.getString("cheng_ban_ting")).replaceAll("");
				String dangShiRen=pattern1.matcher(result.getString("dang_shi_ren")).replaceAll("");
				
				String outLine1=kaiTingRiQi+"\001"+anYou+"\001"+anHao+"\001"
						+faYuanMingCheng+"\001"+shenLiFaTing+"\001"
						+zhuShenFaGuan+"\001"+chengBanTing+"\001"+dangShiRen;
				String md5=DigestUtils.md5Hex(outLine1);
				String[] companyArr=dangShiRen.split("[;,；:：，、.]");
				for(String company:companyArr)
				{
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
						company=company.replace("(", "（").replace(")", "）");
						String key=company+"_"+tableId+"_"+md5;
						String outLine=key+"\001"+company+"\001"+outLine1+"\n";
						fw.write(outLine);
					}
				}
				if((++cnt)%1000==0)
				{
					System.out.println("++"+cnt);
				}
			}
			fw.close();
		}
		catch (Exception e)
		{
			remark1=SysFunc.getError(e);
			jobStatus1=1;
		}

		WriteJobStatus.writeJobStatus("开庭公告解析（类型1）", dt, jobStatus1, remark1.replace("'", ""));
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
		WriteJobStatus.writeJobStatus("开庭公告导入hive（类型1）", dt, jobStatus2, remark2.replace("'", ""));
//		String[] load2HbaseArgs=new String[]
//				{"sh"
//				,"/home/likai/ImportCourtNoticeToHbase.sh"
//				,dt
//				};
//		int jobStatus3=0;
//		String remark3="";
//		try
//		{
//			remark3=ExecShell.exec(load2HbaseArgs);
//		}
//		catch (Exception e)
//		{
//			remark3=SysFunc.getError(e);
//			jobStatus3=1;
//		}
//		WriteJobStatus.writeJobStatus("开庭公告导入hbase（类型1）", dt, jobStatus3, remark3.replace("'", ""));
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, ParseException, InterruptedException
	{
		String[] newArgs={"--startDt=2016-01-01","--stopDt=2016-06-19"};
		JobConfig jobConf=new JobConfig(newArgs);
		CourtNoticeType1Job job =new CourtNoticeType1Job();
//		job.getSelectCmd();
		job.run(jobConf);
	}
}
