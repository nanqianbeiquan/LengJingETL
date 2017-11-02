package tools;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public class ParseDate {

	static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
	static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy/MM/dd");
	static SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy年MM月dd日");
	static SimpleDateFormat sdf4 = new SimpleDateFormat("yyyyMMdd");
	static SimpleDateFormat sdf5 = new SimpleDateFormat("yyyy.MM.dd");
	
	static HashMap<String,String> convertMap=new HashMap<String,String>();
	static 
	{
		convertMap.put("〇", "0");
		convertMap.put("一", "1");
		convertMap.put("二", "2");
		convertMap.put("三", "3");
		convertMap.put("四", "4");
		convertMap.put("五", "5");
		convertMap.put("六", "6");
		convertMap.put("七", "7");
		convertMap.put("八", "8");
		convertMap.put("九", "9");
		convertMap.put("十", "10");
		convertMap.put("十一", "11");
		convertMap.put("十二", "12");
		convertMap.put("十三", "13");
		convertMap.put("十四", "14");
		convertMap.put("十五", "15");
		convertMap.put("十六", "16");
		convertMap.put("十七", "17");
		convertMap.put("十八", "18");
		convertMap.put("十九", "19");
		convertMap.put("二十", "20");
		convertMap.put("二十一", "21");
		convertMap.put("二十二", "23");
		convertMap.put("二十三", "23");
		convertMap.put("二十四", "24");
		convertMap.put("二十五", "25");
		convertMap.put("二十六", "26");
		convertMap.put("二十七", "27");
		convertMap.put("二十八", "28");
		convertMap.put("二十九", "29");
		convertMap.put("三十", "30");
		convertMap.put("三十一", "31");
	}
	
	public static String convertToDigitFormat(String srcDt)
	{
		if(srcDt.startsWith("二〇"))
		{
			int idx1=srcDt.indexOf("年");
			int idx2=srcDt.indexOf("月");
			int idx3=srcDt.indexOf("日");
			if(idx1==4 && (idx2-idx1)>=2 && (idx2-idx1)<=3 && (idx3-idx2)>=2 && (idx3-idx2)<=4)
			{
				String year="";
				for(int i=0;i<idx1;i++)
				{
					year+=convertMap.get(String.valueOf(srcDt.charAt(i)));
				}
				String month=convertMap.get(srcDt.substring(idx1+1,idx2));
				String day=convertMap.get(srcDt.substring(idx2+1,idx3));
				return year+"-"+month+"-"+day;
			}
		}
		return srcDt;
	}
	
	public static String parse(String dt)
	{
		dt=convertToDigitFormat(dt);
		Date date=null;
//		if(date==null)
//		{
//			try
//			{
//				date=sdf1.parse(dt);
//			}
//			catch (Exception e)
//			{
//				date=null;
//			}
//		}
		if(date==null)
		{
			try
			{
				date=sdf2.parse(dt);
			}
			catch (Exception e)
			{
				date=null;
			}
		}
		if(date==null)
		{
			try
			{
				date=sdf3.parse(dt);
			}
			catch (Exception e)
			{
				date=null;
			}
		}
		if(date==null)
		{
			try
			{
				date=sdf4.parse(dt);
			}
			catch (Exception e)
			{
				date=null;
			}
		}
		if(date==null)
		{
			try
			{
				date=sdf5.parse(dt);
			}
			catch (Exception e)
			{
				date=null;
			}
		}
		if(date!=null)
		{
			return sdf1.format(date);
		}
		else
		{
			return dt;
		}
	}
	
	public static void main(String[] args)
	{
		System.out.println(parse("2016-09-12"));
//		System.out.println(parse("2015-06-17"));
//		System.out.println(parse("2015/6/17"));
//		System.out.println(parse("20150617"));
	}
}
