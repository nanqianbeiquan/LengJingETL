package kafka;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import net.sf.json.JSONException;

public class LogConsumer {

	private ConsumerConfig config;
	private String topic;
	private int partitionsNum;
	private MessageExecutor executor;
	private ConsumerConnector connector;
	private ExecutorService threadPool;
//	private Logger neoLog=Logger.getLogger("neo4jUpdate");
//	private Logger neoErrorLog=Logger.getLogger("neo4jUpdateError");
//	private Logger hbaseLog=Logger.getLogger("hbaseUpdate");
//	private Logger hbaseErrorLog=Logger.getLogger("hbaseUpdateError");
//	private Logger recordLost=Logger.getLogger("KafkaRecordLost");
	
	public LogConsumer(String topic,int partitionsNum,MessageExecutor executor) throws Exception{
		Properties properties = new Properties();
		properties.load(ClassLoader.getSystemResourceAsStream("consumer.properties"));
		config = new ConsumerConfig(properties);
		this.topic = topic;
		this.partitionsNum = partitionsNum;
		this.executor = executor;
	}
	
	public void start() throws Exception{
		connector = Consumer.createJavaConsumerConnector(config);
		Map<String,Integer> topics = new HashMap<String,Integer>();
		topics.put(topic, partitionsNum);
		Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(topics);
		List<KafkaStream<byte[], byte[]>> partitions = streams.get(topic);
		threadPool = Executors.newFixedThreadPool(partitionsNum);
		for(KafkaStream<byte[], byte[]> partition : partitions){
			threadPool.execute(new MessageRunner(partition));
		} 
	}

    	
	public void close(){
		try{
			threadPool.shutdownNow();
		}catch(Exception e){
			//
		}finally{
			connector.shutdown();
		}
		
	}
	
	class MessageRunner implements Runnable{
		private KafkaStream<byte[], byte[]> partition;
		
		MessageRunner(KafkaStream<byte[], byte[]> partition) {
			this.partition = partition;
		}
		
		public void run(){
			ConsumerIterator<byte[], byte[]> it = partition.iterator();
			while(it.hasNext()){
				MessageAndMetadata<byte[],byte[]> item = it.next();
				String json=new String(item.message());
				System.out.println(json);	
				
			}
		}
	}
	
	interface MessageExecutor {
		
		public void execute(String message);
	}
	
	public static String printException(Exception e){
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		return sw.toString();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		LogConsumer consumer = null;

		try{		
			MessageExecutor executor = new MessageExecutor() {			
				public void execute(String message) {
					System.out.println("begin :"+message);																
				}
			};
//			consumer = new LogConsumer("zhongshuTest", 3, executor);
			consumer = new LogConsumer("GsSrc21", 3, executor);

			consumer.start();
		}catch(Exception e){
			e.printStackTrace();
		}finally{
//			if(consumer != null){
//				consumer.close();
//			}
		}

	}
	
	

}