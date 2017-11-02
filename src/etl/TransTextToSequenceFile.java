package etl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

import tools.JobConfig;

public class TransTextToSequenceFile {

	public void  run(JobConfig jobConf) throws IOException
	{
		String dt=jobConf.getString("dt");
		Path srcDir=new Path("/crawler/judgment/"+dt);
		Path dstPath=new Path("/crawler/judgment/seq/doc_"+dt+".seq");
//		System.setProperty("hadoop.home.dir", "E:\\EclipseProjects\\CDH5.6");
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(conf);
		
		RemoteIterator<LocatedFileStatus> subFiles = fs.listFiles(srcDir,true);
		int cnt=0;
		SequenceFile.Writer writer = SequenceFile.createWriter(new Configuration(),
				Writer.file(dstPath), Writer.keyClass(Text.class),  
			    Writer.valueClass(Text.class),  
			    Writer.compression(CompressionType.BLOCK));  
		// 通过writer向文档中写入记录  
		while(subFiles.hasNext())
		{
			cnt++;
			LocatedFileStatus subFile = subFiles.next();
			Path subFilePath=subFile.getPath();
			String fileName=subFilePath.getName();
			byte[] fileContent = new byte[(int) subFile.getLen()];
			FSDataInputStream hdfsInStream = fs.open(subFilePath);
			hdfsInStream.read(fileContent);
			writer.append(new Text(fileName.replace(".txt", "")),new Text(fileContent));
			hdfsInStream.close();
			if(cnt%1000==0)
			{
				System.out.println("cnt:"+cnt);
			}
		}
		IOUtils.closeStream(writer);// 关闭write流  
	}
	
	public static void main(String[] args) throws IOException
	{
//		TransTextToSequenceFile job=new TransTextToSequenceFile();
//		job.run();
	}
}
