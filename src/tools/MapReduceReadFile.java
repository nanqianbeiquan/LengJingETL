package tools;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class MapReduceReadFile {  
    

    public static class ReadFileMapper extends  
            Mapper<Text, Text, NullWritable, NullWritable> {  
    	
    	public Log logger=LogFactory.getLog(ReadFileMapper.class);
  
        /* (non-Javadoc) 
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context) 
         */  
        @Override  
        public void map(Text key, Text value, Context context) {  
        	
        	logger.info("key -> "+key.toString());
        	logger.info("value -> "+value.toString());
        }  
  
    }  
    /** 
     * @param args 
     * @throws IOException 
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */  
    public void run() throws IOException, InterruptedException, ClassNotFoundException {  
          
    	Configuration conf=new Configuration();
        Job job = Job.getInstance(conf,"read seq file");  
        job.setJarByClass(MapReduceReadFile.class);  
        job.setMapperClass(ReadFileMapper.class);
        job.setMapOutputValueClass(NullWritable.class);  
        Path path = new Path("/crawler/judgment/likaitest/");
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, path);
        
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class);
        System.exit(job.waitForCompletion(true)?0:1);  
    }  
  
} 