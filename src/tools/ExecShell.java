package tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

public class ExecShell {
	
	/**
	 * 
	 * @param cmd
	 * @return 返回执行结果
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static String exec(String cmd) throws IOException, InterruptedException
	{
		System.out.println(cmd);
		Process pro = Runtime.getRuntime().exec(cmd);
		pro.waitFor();  
        InputStream input = pro.getInputStream();
        InputStream error = pro.getErrorStream();
        String inputContent=processStdout(input);
        String errorContent=processStdout(error);
        if(inputContent.length()>0)
        System.out.println("--------------------------------stdout----------------------------------\n"+inputContent
        				+"\n========================================================================");
        if(errorContent.length()>0)
        System.out.println("--------------------------------error-----------------------------------\n"+errorContent
        				+"\n========================================================================");
        return inputContent+"\n"+errorContent;
	}
	
	public static String exec(String[] args) throws IOException, InterruptedException
	{
		System.out.println(Arrays.toString(args));
		Process pro = Runtime.getRuntime().exec(args);
		pro.waitFor();  
        InputStream input = pro.getInputStream();
        InputStream error = pro.getErrorStream();
        String inputContent=processStdout(input);
        String errorContent=processStdout(error);
        System.out.println("--------------------------------stdout----------------------------------\n"+inputContent
        				+"\n========================================================================");
        System.out.println("--------------------------------error-----------------------------------\n"+errorContent
        				+"\n========================================================================");
        return inputContent+"\n"+errorContent;
	}
	
	public static String processStdout(InputStream is) {
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		StringBuilder builder = new StringBuilder();
		String line = null;
		try {
	        while ((line = reader.readLine()) != null) {
	        	builder.append(line);
	        	builder.append("\n"); //appende a new line

	        }
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		}
		return builder.toString();
	}
	
	public static void main(String[] args) throws IOException, InterruptedException
	{
//		System.out.println(exec(new String[]{"cat","/Users/likai/Documents/other/tt=tt.txt"}));
		System.out.println(exec("python /Users/likai/Documents/test.py companyName=上海正地实业有限公司 taskId=123 accountId=456"));
	}
}
