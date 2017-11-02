package tools;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Parsemoney extends UDF{
	
	private static Map<String,Float> map=new HashMap<String,Float>();
	static{
		map.put("人民币",10000f);
		map.put("元",10000f);
		map.put("元人民币",10000f);
		map.put("元港币",10000/0.843453f);
		map.put("元人民币", 10000f);
		map.put("美元", 10000/6.5535f);
		map.put("元美元", 10000/6.551900f);
		map.put("人民币元", 10000f);
		map.put("港币",10000/0.843453f);
		map.put("澳大利亚元",10000/4.692800f);
		map.put("欧元",10000/7.322113f);
		map.put("元欧元（原欧洲货币单位）",10000/7.322113f);
		map.put("港元", 10000/0.843453f);
		map.put("元港元（港币）",10000/0.843453f);
		map.put("元港元",10000/0.843453f);
		map.put("日元", 10000/0.059793f);
		map.put("元日元", 10000/0.059793f);
		map.put("新加坡元",10000/4.737076f);
		map.put("加拿大元",10000/4.972281f);
		map.put("元加拿大元",10000/4.972281f);
		map.put("英镑", 10000/9.530964f);
		map.put("元美元 美元",10000/6.551900f);
		map.put("元人民币元", 10000f);
		map.put("香港元", 10000/0.843453f);
		map.put("E人民币",0.00001f);
		map.put("E-",0.00001f);
		map.put("元 美元",10000/6.551900f);
		map.put("加元",10000/4.972281f);
		map.put("瑞士法郎",10000/6.602900f);
		map.put("元瑞士法郎",10000/6.602900f);
		map.put("德国马克",10000/3.482f);
		map.put("元德国马克",10000/3.482f);
		map.put("德拉克马",10000/3.482f);
		map.put("元德国马克",10000/3.482f);
		map.put("法国法郎",10000/1.1376f);
		map.put("元法国法郎",10000/1.1376f);
		map.put("E-人民币",0.00001f);
		map.put("元欧元",10000/7.322113f);
		map.put("元日元",10000/0.059793f);
		map.put("元澳大利亚元",10000/4.692800f);
		map.put("澳元", 10000/4.692800f);
		map.put("元加元", 10000/4.972281f);
		map.put("元法国法郎",10000/1.1376f);
		map.put("挪威克朗",10000/0.7810f);
		map.put("元挪威克朗",10000/0.7810f);
		map.put("韩国圆", 10000/0.0055f);
		map.put("韩元", 10000/0.0055f);
		map.put("元韩国圆",10000/0.0055f);
		map.put("元",10000f);
		map.put("元 意大利里拉",10000/0.43994721f);
		map.put("瑞典克郎",10000/0.7838f);
		map.put("瑞典克朗",10000/0.7838f);
		map.put("元瑞典克郎",10000/0.7838f);
		map.put("荷兰",10000/3.3183f);
		map.put("芬兰马克", 10000/1.1839f);
		map.put("韩国元",10000/0.0055f);
		map.put("元澳门元",10000/0.8199f);
		map.put("澳门元", 10000/0.8199f);
		map.put("缅元",10000/0.0056f);
		map.put("阿根廷比索",10000/0.4690f);
		map.put("阿根廷比索",10000/0.0649f);
		map.put("哥伦比亚比索",10000/0.0021f);
		map.put("元哥伦比亚比索",10000/0.0021f);
		map.put("元比利时法郎",10000/0.1937f);
		map.put("元新台币",10000/0.2084f);
		map.put("新台币",10000/0.2084f);
		map.put("元新加坡元",10000/4.7565f);
		map.put("元英镑",10000/9.530964f);
		map.put("元香港元",10000/0.843453f);
		map.put("元新西兰元",10000/4.4779f);
		map.put("新西兰元",10000/4.4779f);
		map.put("元阿富汗尼",10000/0.129f);
		map.put("元阿富汗尼",10000/0.129f);
		map.put("元阿富汗尼", 10000/12.5454f);
		map.put("丹麦克朗",10000/0.9857f);
		map.put("元丹麦克朗",10000/0.9857f);
		map.put("元丹麦克朗", 10000/0.5582f);
		map.put("元泰国铢",10000/0.190f);
		map.put("新台湾元",10000/0.2006f);
		map.put("元玻利维亚比索", 10000/0.962472255f);
		map.put("元列弗", 10000/3.747f);
		map.put("元开曼群岛元",10000/8.08f);
		map.put("智利比索",10000/0.0094f);
		map.put("元智利比索", 10000/0.0094f);
		map.put("元卢比", 10000/0.0969f);
		map.put("元马来西亚林吉特",10000/1.5910f);
		map.put("马来西亚林吉特",10000/1.5910f);
		map.put("元布隆迪法郎",10000/0.0040f);
		map.put("阿富汗尼", 10000/0.0952f);
		map.put("元新克瓦查",10000/0.0012f);
		map.put("元塔拉",10000/2.5322f);
		map.put("元塔拉",10000/2.5322f);
		map.put("元阿富汗尼",10000/0.095102201736f);
		map.put("元阿富汗尼",10000/0.095102201736f);
		map.put("元阿富汗尼", 10000/0.095102201736f);
		map.put("新扎伊尔尼", 10000/13.1038f);
		map.put("元新扎伊尔尼",10000/13.1038f);

	}
	
	public String evaluate(String zb,String bzType){
		String money=zb.toString();
		String bztype=bzType.toString();
	    String type="";
		if(bztype!=null && !bztype.equals("")){
			System.out.println("AA:"+bztype.replaceAll("\\s", "").replace(".", ""));
			if(map.containsKey(bztype.replaceAll("\\s", "").replace(".", ""))){
				Pattern pattern = Pattern.compile("[0-9].*|[0-9]*"); 
				   Matcher isNum = pattern.matcher(money);
				   if(isNum.matches() && (money!=null && !"".equals(money))){
				    Float d=map.get(bztype.replaceAll("\\s", "").replace(".", ""));
				    return String.format("%.2f",Double.parseDouble(money)/d);
				   }else{
					  return "";
				   }
			}else{
				return "";
			}
		}else{
			return "";
		}
	}
	public static void main(String[] args) {
		Parsemoney money=new Parsemoney();
		System.out.println(money.evaluate("", "美元"));
	}

}
