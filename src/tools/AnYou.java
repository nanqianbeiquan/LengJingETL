package tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

public class AnYou {

//	static TreeSet<String> anYouSet=new TreeSet<String>();
	static TreeMap<String,String> anYouMap=new TreeMap<String,String>();
	static
	{
		loadAnYou();
	}
	
	static void loadAnYou() 
	{
		
		File src=new File("conf/dict/an_you.dic");
		InputStreamReader read;
		try {
			read = new InputStreamReader(new FileInputStream(src));
			BufferedReader bufferedReader = new BufferedReader(read);
			String lineText = null;
			while((lineText = bufferedReader.readLine()) != null)
			{
				String anYou=lineText.split("\t")[0];
//				anYouSet.add(anYou);
				
				String key=String.format("%02d|%s", 100-anYou.length(),anYou);
				anYouMap.put(key, anYou);
				
//				System.out.println(key);
			}
			bufferedReader.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	public static String getAnYou(String content)
	{
		String res=null;
		Iterator<Entry<String, String>> iterator = anYouMap.entrySet().iterator();
		while(iterator.hasNext())
		{
			String anYou=iterator.next().getValue();
			if(content.contains(anYou) && !content.contains(anYou+"局") && !content.contains(anYou+"总局"))
			{
				res=anYou;
				break;
			}
		}
		return res;
	}
	
	public static void main(String[] args)
	{
		System.out.println(
				getAnYou("我院定于二〇一四年六月十日 上午十时整，在本院一区第15法庭依法公开宣判张传统强制猥亵妇女罪一案。")
						);
		
	}
}
