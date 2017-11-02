package notice;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.text.AbstractDocument.Content;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.apache.commons.lang3.StringUtils;

import tools.AnYou;

public class BjCourtNotice {
	
//	申请再审人|原审(一审)诉讼地位|抗诉机关|诉讼|反诉|被申诉人|申诉人|再审|申请人|原审|被上诉人|上诉人|被告人|原告|被告|第三人|被
	static Pattern pattern1 = Pattern.compile("[〇一二三四五六七八九]{4}年[一二三四五六七八九十]{1,2}月[一二三四五六七八九十]{1,3}日");
	static Pattern pattern2 = Pattern.compile("在[\\s\\S]*依法");

	public static HashMap<String, String> parse(String content)
	{
		if(content.equals("") || content.contains("现取消开庭"))
		{
			return null;
		}
		if(content.endsWith("）"))
		{
			content=content.substring(0,content.lastIndexOf("（"));
		}
		if(!content.endsWith("。"))
		{
			content+="。";
		}
		if(content.endsWith("案件。"))
		{
			content=content.replace("案件。", "一案。");
		}
		if(!content.endsWith("案。"))
		{
			content=content.replace("。", "案。");
		}
		
		HashMap<String,String> parseResult=new HashMap<String,String>();
		String kaiTingRiQi = "";
		String shenLiFaTing="";
		String dangShiRen="";
		String anYou=AnYou.getAnYou(content);
		Matcher m1 = pattern1.matcher(content);
		if (m1.find()) {
			kaiTingRiQi=m1.group();        
        }
		
		Matcher m2 = pattern2.matcher(content);
		if (m2.find()) {
			shenLiFaTing=m2.group();
			shenLiFaTing=shenLiFaTing.replace("依法", "").replace("在","").replace("本院", "");
        } 
		int dangShiRenStartIdx=-1;
		int dangShiRenStopIdx=-1;
		if(dangShiRenStartIdx==-1)
		{
			dangShiRenStartIdx=content.indexOf("宣判");
			if(dangShiRenStartIdx!=-1)
			{
				dangShiRenStartIdx=dangShiRenStartIdx+"宣判".length();
			}
		}
		
		if(dangShiRenStartIdx==-1)
		{
			dangShiRenStartIdx=content.indexOf("审理");
			if(dangShiRenStartIdx!=-1)
			{
				dangShiRenStartIdx=dangShiRenStartIdx+"审理".length();
			}
		}
		if(dangShiRenStartIdx==-1)
		{
			dangShiRenStartIdx=content.indexOf("询问");
			if(dangShiRenStartIdx!=-1)
			{
				dangShiRenStartIdx=dangShiRenStartIdx+"询问".length();
			}
		}
		if(dangShiRenStartIdx==-1)
		{
			dangShiRenStartIdx=content.indexOf("召开庭前会议");
			if(dangShiRenStartIdx!=-1)
			{
				dangShiRenStartIdx=dangShiRenStartIdx+"召开庭前会议".length();
			}
		}
		if(anYou!=null)
		{
			dangShiRenStopIdx=content.indexOf(anYou);
			if(dangShiRenStopIdx!=-1)
			{
				Pattern pattern3 = Pattern.compile("(涉嫌犯)?"+anYou+"[ ]?[罪]?(再审|上诉|裁决)?(暨)?(附带民事)?(赔偿|诉讼)?[〇一二三四五六七八九十0123456789两]+案。");
				Matcher m3 = pattern3.matcher(content);
				if(!m3.find())
				{
					dangShiRenStopIdx=-1;
				}
				else
				{
					String stopFlag=m3.group();
					dangShiRenStopIdx=content.indexOf(stopFlag);
				}
			}
		}
		
		if(dangShiRenStopIdx==-1)
		{
			Pattern pattern4 = Pattern.compile("[因]?(其他)?不服");
			Matcher m4 = pattern4.matcher(content);
			if(m4.find())
			{
				anYou=m4.group();
				dangShiRenStopIdx=content.indexOf(anYou);
				Pattern pattern5 = Pattern.compile("(上诉|再审|裁决)?[〇一二三四五六七八九0123456789两]+案。");
				Matcher m5 = pattern5.matcher(content);
				if(m5.find())
				{
					String endFlag=m5.group();
					anYou=content.substring(content.indexOf(anYou),content.indexOf(endFlag));
				}
				else
				{
					anYou="";
				}
			}
		}
		if(dangShiRenStartIdx!=-1 && dangShiRenStopIdx!=-1)
		{
			dangShiRen=content.substring(dangShiRenStartIdx,dangShiRenStopIdx);
			dangShiRen=dangShiRen.replace(" ", "");
		}
		
		if(dangShiRen.endsWith("与"))
		{
			dangShiRen=dangShiRen.substring(0, dangShiRen.length()-1);
		}
		
//		System.out.println(anYou);
//		System.out.println(kaiTingRiQi);
//		System.out.println(shenLiFaTing);
//		System.out.println(dangShiRen);
		
		parseResult.put("案由", anYou);
		parseResult.put("当事人", dangShiRen);
		parseResult.put("审理法庭", shenLiFaTing);
		parseResult.put("开庭日期", kaiTingRiQi);
		
		return parseResult;
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException
	{
		System.out.println(parse("我院定于二〇一六年三月二日 下午十三时四十分，在本院天竺三法庭依法公开开庭审理陈世田、李怀臣、王海生、王仰斌、王洋、王海龙、王占国、王小军、马志杰、刘东阳与郭兵、戴绪龙、代同顺劳务合同纠纷十案。"));
//		System.out.println(test("我院定于二〇一四年十月三十一日 上午八时三十分，在本院第三法庭依法公开开庭审理甘甜与赵钊离婚纠纷一案。"));
		
//		Pattern pattern1 = Pattern.compile("[〇一二三四五六七八九]{4}年[一二三四五六七八九十]{1,2}月[一二三四五六七八九十]{1,3}日");
//		Pattern pattern1 = Pattern.compile("(二〇一四)+");
//		Matcher m = pattern1.matcher("我院定于二〇一四年十月三十一日 上午八时三十分，在本院第三法庭依法公开开庭审理甘甜与赵钊离婚纠纷一案。");
//		while (m.find()) {
//			System.out.println(m.group());        
//        } 
		String c="我院定于二〇一五年十一月二十五日 上午十时二十分，在本院第二十法庭依法公开开庭审理梁金全交通肇事罪一案。（本案适用速裁程序审理,送达期限不受刑事诉讼法规定的限制）";
		System.out.println(c.substring(0,c.lastIndexOf("（")));
		
	}

}

