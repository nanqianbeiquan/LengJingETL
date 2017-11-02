package etl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MergePersonNodesReducer  extends Reducer<Text,Text,NullWritable,Text>{

	public void setup(Context context) throws IOException, InterruptedException
	{
		super.cleanup(context);
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		super.cleanup(context);
	}

	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
		
		String person=key.toString();
		ArrayList<HashSet<String>> groupList=new ArrayList<HashSet<String>>();
        HashMap<String,Integer> mc2GroupId=new HashMap<String,Integer>();
		for(Text value:values)
		{
			String[] companys=value.toString().split("\001");
			String company1=companys[0];
        	String company2=companys[1];
        	int groupId=-1;
        	if(mc2GroupId.containsKey(company1) && mc2GroupId.containsKey(company2))
        	{
        		groupId=mc2GroupId.get(company1);
        		int setIdx2=mc2GroupId.get(company2);
        		if(groupId!=setIdx2)
        		{
        			for(String company:groupList.get(setIdx2))
            		{
            			mc2GroupId.put(company,groupId);
            		}
            		groupList.get(groupId).addAll(groupList.get(setIdx2));
            		groupList.set(setIdx2, null);
        		}
        	}
        	else if(mc2GroupId.containsKey(company1) && !mc2GroupId.containsKey(company2))
        	{
        		groupId=mc2GroupId.get(company1);
        		groupList.get(groupId).add(company2);
        		mc2GroupId.put(company2, groupId);
        	}
        	else if(mc2GroupId.containsKey(company2) && !mc2GroupId.containsKey(company1))
        	{
        		groupId=mc2GroupId.get(company2);
        		groupList.get(groupId).add(company1);
        		
        		mc2GroupId.put(company1, groupId);
        	}
        	else
        	{
        		groupId=groupList.size();
        		groupList.add(new HashSet<String>());
        		groupList.get(groupId).add(company1);
        		groupList.get(groupId).add(company2);
        		mc2GroupId.put(company1, groupId);
        		mc2GroupId.put(company2, groupId);
        	}
		}
		
		int groupId=0;
		for(HashSet<String> companySet:groupList)
        {
			if(companySet!=null)
        	{
				groupId++;
				String uid=person+"_"+groupId;
				for(String company:companySet)
        		{
					context.write(NullWritable.get(),new Text(uid+'\001'+person+'\001'+company));
        		}
        	}
        }
		
		
	}
	
	public static void test() throws IOException
	{
		File src=new File("conf/周成建测试");
		InputStreamReader read = new InputStreamReader(new FileInputStream(src));
        BufferedReader bufferedReader = new BufferedReader(read);
        String lineText = null;
        
        
        ArrayList<HashSet<String>> mergeResultList=new ArrayList<HashSet<String>>();
        HashMap<String,Integer> mergeResultMap=new HashMap<String,Integer>();
        while((lineText = bufferedReader.readLine()) != null)
        {
//        	System.out.println(lineText);
        	String[] companys=lineText.toString().split("\t");
        	String company1=companys[0];
        	String company2=companys[1];
        	int setIdx=-1;
        	if(mergeResultMap.containsKey(company1) && mergeResultMap.containsKey(company2))
        	{
        		setIdx=mergeResultMap.get(company1);
        		int setIdx2=mergeResultMap.get(company2);
        		if(setIdx!=setIdx2)
        		{
        			for(String company:mergeResultList.get(setIdx2))
            		{
            			mergeResultMap.put(company,setIdx);
            		}
            		mergeResultList.get(setIdx).addAll(mergeResultList.get(setIdx2));
            		mergeResultList.set(setIdx2, null);
        		}
        	}
        	else if(mergeResultMap.containsKey(company1) && !mergeResultMap.containsKey(company2))
        	{
        		setIdx=mergeResultMap.get(company1);
        		mergeResultList.get(setIdx).add(company2);
        		
        		mergeResultMap.put(company2, setIdx);
        	}
        	else if(mergeResultMap.containsKey(company2) && !mergeResultMap.containsKey(company1))
        	{
        		setIdx=mergeResultMap.get(company2);
        		mergeResultList.get(setIdx).add(company1);
        		
        		mergeResultMap.put(company1, setIdx);
        	}
        	else
        	{
        		setIdx=mergeResultList.size();
        		mergeResultList.add(new HashSet<String>());
        		mergeResultList.get(setIdx).add(company1);
        		mergeResultList.get(setIdx).add(company2);
        		mergeResultMap.put(company1, setIdx);
        		mergeResultMap.put(company2, setIdx);
        	}
//        	System.out.println(mergeResultList);
        }
        read.close();
        System.out.println(mergeResultMap.size());
//        System.out.println(mergeResultList);
        for(HashSet<String> s:mergeResultList)
        {
        	if(s!=null)
        	{
        		System.out.println(s);
        	}
        }
	}
	
	public static void main(String[] args) throws IOException
	{
		test();
	}
	
}
