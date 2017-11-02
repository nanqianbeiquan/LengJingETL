package kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import com.alibaba.fastjson.JSONObject;

public class LoadToNewRegisteredCompany {

	static HashMap<String,String> provCodeMap=new HashMap<String,String>();
	static
	{
		provCodeMap.put("10","工商总局");
		provCodeMap.put("11","北京市");
		provCodeMap.put("12","天津市");
		provCodeMap.put("13","河北省");
		provCodeMap.put("14","山西省");
		provCodeMap.put("15","内蒙古自治区");
		provCodeMap.put("21","辽宁省");
		provCodeMap.put("22","吉林省");
		provCodeMap.put("23","黑龙江省");
		provCodeMap.put("31","上海市");
		provCodeMap.put("32","江苏省");
		provCodeMap.put("33","浙江省");
		provCodeMap.put("34","安徽省");
		provCodeMap.put("35","福建省");
		provCodeMap.put("36","江西省");
		provCodeMap.put("37","山东省");
		provCodeMap.put("41","河南省");
		provCodeMap.put("42","湖北省");
		provCodeMap.put("43","湖南省");
		provCodeMap.put("44","广东省");
		provCodeMap.put("45","广西壮族自治区");
		provCodeMap.put("46","海南省");
		provCodeMap.put("50","重庆市");
		provCodeMap.put("51","四川省");
		provCodeMap.put("52","贵州省");
		provCodeMap.put("53","云南省");
		provCodeMap.put("54","西藏自治区");
		provCodeMap.put("61","陕西省");
		provCodeMap.put("62","甘肃省");
		provCodeMap.put("63","青海省");
		provCodeMap.put("64","宁夏回族自治区");
		provCodeMap.put("65","新疆维吾尔自治区");
	}
	
	public static int loadWithZch(String path) throws IOException
	{
		KafkaAPI kafka=new KafkaAPI();
		kafka.initProducer();
		
		File src=new File(path);
		InputStreamReader read = new InputStreamReader(new FileInputStream(src));
        BufferedReader bufferedReader = new BufferedReader(read);
        String lineText = null;
        while((lineText = bufferedReader.readLine()) != null)
        {
        	String[] vals=lineText.split("\t");
        	String name=vals[0];
        	String zch=vals[1];
        	String provCode=null;
        	if(zch.length()==18)
        	{
        		provCode=zch.substring(2,4);
        	}
        	else
        	{
        		provCode=zch.substring(0,2);
        	}
        	String province=provCodeMap.get(provCode);
        	System.out.println(name+" -> "+province);
//        	if(province!=null)
//        	{
//        		JSONObject json=new JSONObject();
//            	json.put("companyName", name);
//            	json.put("province", provCode);
//            	String jsonText=json.toJSONString();
//            	String topic="NewRegisteredCompany";
//            	
//            	if(vals[1].equals("浙江省") || vals[1].equals("北京市"))
//            	{
//            		topic="NewRegisteredCompanyLockId";
//            	}
//            	kafka.send(topic, jsonText);
//        	}
        	
        }
        bufferedReader.close();
        kafka.producer.close();
		return 0;
	}
	
	public static void addToTopic(String companyName,String province) throws IOException
	{
		KafkaAPI kafka=new KafkaAPI();
		kafka.initProducer();
		JSONObject json=new JSONObject();
    	json.put("companyName", companyName);
    	json.put("province", province);
    	String jsonText=json.toJSONString();
//    	System.out.println(jsonText);
    	String topic="NewRegisteredCompany";
    	
    	if(province.equals("浙江省") || province.equals("北京市"))
    	{
    		topic="NewRegisteredCompanyLockId";
    	}
    	kafka.send(topic, jsonText);
		kafka.producer.close();
	}
	
	public static int loadWithProvince(String path) throws IOException
	{
		KafkaAPI kafka=new KafkaAPI();
		kafka.initProducer();
		
		File src=new File(path);
		InputStreamReader read = new InputStreamReader(new FileInputStream(src));
        BufferedReader bufferedReader = new BufferedReader(read);
        String lineText = null;
        while((lineText = bufferedReader.readLine()) != null)
        {
        	String[] vals=lineText.split("\t");
        	JSONObject json=new JSONObject();
        	json.put("companyName", vals[0]);
        	json.put("province", vals[1]);
        	String jsonText=json.toJSONString();
//        	System.out.println(jsonText);
        	String topic="NewRegisteredCompany";
        	
        	if(vals[1].equals("浙江省") || vals[1].equals("北京市"))
        	{
        		topic="NewRegisteredCompanyLockId";
        	}
        	kafka.send(topic, jsonText);
        }
        bufferedReader.close();
        kafka.producer.close();
		return 0;
	}
	
	public static void main(String[] args) throws IOException
	{
		loadWithProvince("data/新注册_20160920_浙江.txt");
//		loadWithZch("data/新注册_20160913_全国.txt");
//		addToTopic("苏州中晟宏芯信息科技有限公司","江苏省");
//		addToTopic("苏州中晟宏芯信息科技有限公司","江苏省");
	}
}
