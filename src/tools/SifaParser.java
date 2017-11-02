package tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class SifaParser {

	public SifaParser()
	{
		
	}
	
	public void parse(String doc)
	{
		
	}
	
	public void test() throws IOException
	{
		File src=new File("data/f51f0739-08b3-46a0-bd1b-aadbef75056b.txt");
		InputStreamReader reader = new InputStreamReader(new FileInputStream(src));
        char[] content=new char[(int) src.length()];
        reader.read(content);
        reader.close();
        String doc=new String(content);
        System.out.println(doc);
        parse(doc);
	}
	
	public static void main(String[] args) throws IOException
	{
		SifaParser parser=new SifaParser();
		parser.test();
		
	}
}
