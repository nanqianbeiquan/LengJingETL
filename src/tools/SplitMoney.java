package tools;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SplitMoney{
	
	private static Map<String,Double> huilv=new HashMap<String,Double>();
	static{
		huilv.put("人民币",10000d);
		huilv.put("元",10000d);
		huilv.put("元人民币",10000d);
		huilv.put("元港币",10000/0.843453d);
		huilv.put("元人民币", 10000d);
		huilv.put("美元", 10000/6.5535d);
		huilv.put("元美元", 10000/6.551900d);
		huilv.put("人民币元", 10000d);
		huilv.put("港币",10000/0.843453d);
		huilv.put("澳大利亚元",10000/4.692800d);
		huilv.put("欧元",10000/7.322113d);
		huilv.put("元欧元（原欧洲货币单位）",10000/7.322113d);
		huilv.put("港元", 10000/0.843453d);
		huilv.put("元港元（港币）",10000/0.843453d);
		huilv.put("元港元",10000/0.843453d);
		huilv.put("日元", 10000/0.059793d);
		huilv.put("元日元", 10000/0.059793d);
		huilv.put("新加坡元",10000/4.737076d);
		huilv.put("加拿大元",10000/4.972281d);
		huilv.put("元加拿大元",10000/4.972281d);
		huilv.put("英镑", 10000/9.530964d);
		huilv.put("元美元 美元",10000/6.551900d);
		huilv.put("元人民币元", 10000d);
		huilv.put("香港元", 10000/0.843453d);
		huilv.put("E人民币",0.00001d);
		huilv.put("E-",0.00001d);
		huilv.put("元 美元",10000/6.551900d);
		huilv.put("加元",10000/4.972281d);
		huilv.put("瑞士法郎",10000/6.602900d);
		huilv.put("元瑞士法郎",10000/6.602900d);
		huilv.put("德国马克",10000/3.482d);
		huilv.put("元德国马克",10000/3.482d);
		huilv.put("德拉克马",10000/3.482d);
		huilv.put("元德国马克",10000/3.482d);
		huilv.put("法国法郎",10000/1.1376d);
		huilv.put("元法国法郎",10000/1.1376d);
		huilv.put("E-人民币",0.00001d);
		huilv.put("元欧元",10000/7.322113d);
		huilv.put("元日元",10000/0.059793d);
		huilv.put("元澳大利亚元",10000/4.692800d);
		huilv.put("澳元", 10000/4.692800d);
		huilv.put("元加元", 10000/4.972281d);
		huilv.put("元法国法郎",10000/1.1376d);
		huilv.put("挪威克朗",10000/0.7810d);
		huilv.put("元挪威克朗",10000/0.7810d);
		huilv.put("韩国圆", 10000/0.0055d);
		huilv.put("韩元", 10000/0.0055d);
		huilv.put("元韩国圆",10000/0.0055d);
		huilv.put("元",10000d);
		huilv.put("元 意大利里拉",10000/0.43994721d);
		huilv.put("瑞典克郎",10000/0.7838d);
		huilv.put("瑞典克朗",10000/0.7838d);
		huilv.put("元瑞典克郎",10000/0.7838d);
		huilv.put("荷兰",10000/3.3183d);
		huilv.put("芬兰马克", 10000/1.1839d);
		huilv.put("韩国元",10000/0.0055d);
		huilv.put("元澳门元",10000/0.8199d);
		huilv.put("澳门元", 10000/0.8199d);
		huilv.put("缅元",10000/0.0056d);
		huilv.put("阿根廷比索",10000/0.4690d);
		huilv.put("阿根廷比索",10000/0.0649d);
		huilv.put("哥伦比亚比索",10000/0.0021d);
		huilv.put("元哥伦比亚比索",10000/0.0021d);
		huilv.put("元比利时法郎",10000/0.1937d);
		huilv.put("元新台币",10000/0.2084d);
		huilv.put("新台币",10000/0.2084d);
		huilv.put("元新加坡元",10000/4.7565d);
		huilv.put("元英镑",10000/9.530964d);
		huilv.put("元香港元",10000/0.843453d);
		huilv.put("元新西兰元",10000/4.4779d);
		huilv.put("新西兰元",10000/4.4779d);
		huilv.put("元阿富汗尼",10000/0.129d);
		huilv.put("元阿富汗尼",10000/0.129d);
		huilv.put("元阿富汗尼", 10000/12.5454d);
		huilv.put("丹麦克朗",10000/0.9857d);
		huilv.put("元丹麦克朗",10000/0.9857d);
		huilv.put("元丹麦克朗", 10000/0.5582d);
		huilv.put("元泰国铢",10000/0.190d);
		huilv.put("新台湾元",10000/0.2006d);
		huilv.put("元玻利维亚比索", 10000/0.962472255d);
		huilv.put("元列弗", 10000/3.747d);
		huilv.put("元开曼群岛元",10000/8.08d);
		huilv.put("智利比索",10000/0.0094d);
		huilv.put("元智利比索", 10000/0.0094d);
		huilv.put("元卢比", 10000/0.0969d);
		huilv.put("元马来西亚林吉特",10000/1.5910d);
		huilv.put("马来西亚林吉特",10000/1.5910d);
		huilv.put("元布隆迪法郎",10000/0.0040d);
		huilv.put("阿富汗尼", 10000/0.0952d);
		huilv.put("元新克瓦查",10000/0.0012d);
		huilv.put("元塔拉",10000/2.5322d);
		huilv.put("元塔拉",10000/2.5322d);
		huilv.put("元阿富汗尼",10000/0.095102201736d);
		huilv.put("元阿富汗尼",10000/0.095102201736d);
		huilv.put("元阿富汗尼", 10000/0.095102201736d);
		huilv.put("新扎伊尔尼", 10000/13.1038d);
		huilv.put("元新扎伊尔尼",10000/13.1038d);
	}
	
	static HashMap<String,String> currencyMap=new HashMap<String,String>();
	static
	{
		currencyMap.put("万元","万元");
		currencyMap.put("万元港币","万港币");
		currencyMap.put("万阿富汗尼","万阿富汗尼");
		currencyMap.put("万元欧元","万欧元");
		currencyMap.put("万元澳大利亚元","万澳大利亚元");
		currencyMap.put("万法国法郎","万法国法郎");
		currencyMap.put("万香港元","万港币");
		currencyMap.put("万马来西亚林吉特","万马来西亚林吉特");
		currencyMap.put("万加拿大元","万加拿大元");
		currencyMap.put("万元新加坡元","万新加坡元");
		currencyMap.put("万元人民币","万元");
		currencyMap.put("万德拉克马","万德拉克马");
		currencyMap.put("万人民币","万元");
		currencyMap.put("万新西兰元","万新西兰元");
		currencyMap.put("万人民币元","万元");
		currencyMap.put("万瑞典克朗","万瑞典克朗");
		currencyMap.put("万美元","万美元");
		currencyMap.put("万加元","万加拿大元");
		currencyMap.put("万元加拿大元","万加拿大元");
		currencyMap.put("万元英镑","万英镑");
		currencyMap.put("万澳大利亚元","万澳大利亚元");
		currencyMap.put("万港元","万港币");
		currencyMap.put("万瑞士法郎","万瑞士法郎");
		currencyMap.put("万韩元","万韩元");
		currencyMap.put("万新台币","万新台币");
		currencyMap.put("万英镑","万英镑");
		currencyMap.put("万元美元","万美元");
		currencyMap.put("万港币","万港币");
		currencyMap.put("万欧元","万欧元");
		currencyMap.put("万元日元","万日元");
		currencyMap.put("万新加坡元","万新加坡元");
		currencyMap.put("万德国马克","万德国马克");
		currencyMap.put("万元港元（港币）","万港币");
		currencyMap.put("万元港元","万港币");
		currencyMap.put("万日元","万日元");
		currencyMap.put("万元欧元（原欧洲货币单位）","万欧元");
		currencyMap.put("万瑞典克郎","万瑞典克朗");
		currencyMap.put("万","万元");
		currencyMap.put("人民币元","元");
		currencyMap.put("人民币","元");
		currencyMap.put("元人民币","元");
		currencyMap.put("元","元");
		currencyMap.put("亿元","亿元");
		currencyMap.put("亿","亿元");
	}
	
	static Set<Character> ignoreSet=new HashSet<Character>();
	static
	{
		ignoreSet.add(' ');
		ignoreSet.add('\t');
		ignoreSet.add('\n');
		ignoreSet.add(',');
		ignoreSet.add(';');
		ignoreSet.add('&');
		ignoreSet.add('n');
		ignoreSet.add('b');
		ignoreSet.add('s');
		ignoreSet.add('p');
	}
	
	public static String[] evaluate(String money)
	{
		StringBuilder amountBuilder=new StringBuilder();
		StringBuilder currencyBuilder=new StringBuilder();	
		for(int i=0;i<money.length();i++)
		{
			char c=money.charAt(i);
			if(!ignoreSet.contains(c))
			{
				if((c>=48 && c<=57) || c==46)
				{
					
					if(currencyBuilder.length()==0)
					{
						amountBuilder.append(c);
					}
					else
					{
						break;
					}
				}
				else
				{
					currencyBuilder.append(c);
				}
			}
		}
		String currency=String.valueOf(currencyMap.get(currencyBuilder.toString()));
		String amount=amountBuilder.toString();
		if(currency.equals("null") && !amount.equals(""))
		{
			currency="万元";
		}
//		System.out.println(amount+","+currency);
		
		
		if(!amount.equals("") && currency.equals("亿元"))
		{
			amount=String.valueOf(Double.valueOf(amount)*10000);
			currency="万元";
		}
		else if(!amount.equals("") && currency.equals("元"))
		{
			amount=String.valueOf(Double.valueOf(amount)/10000);
			currency="万元";
		}
		String currency2=currency.replace("万","");
		if(!amount.equals("") && huilv.containsKey(currency2))
		{
			amount=String.valueOf(10000*Double.valueOf(amount)/huilv.get(currency2));
		}
		else if(!amount.equals("") && !huilv.containsKey(currency2))
		{
			throw new IllegalArgumentException("未知汇率"+currency2);
		}
//		System.out.println(amount);
		return new String[]{amount,currency};
	}
	
	public static void main(String[] args)
	{
		System.out.println(Arrays.asList(evaluate("122.223545")));
//		HashSet<String> set=new HashSet<String>();
//		for(String s:currencyMap.values())
//			set.add(s.replace("万", ""));
//		System.out.println(set);
	}
	
	
}
