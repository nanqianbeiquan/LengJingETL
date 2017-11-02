package kafka;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.alibaba.fastjson.JSONObject;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaAPI {

	public KafkaProducer<String,String> producer;
	ConsumerConnector consumer;
	public KafkaAPI() throws IOException  
	{
		
		
	}
	
	public void initProducer() throws IOException
	{
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "hadoop10:9092,hadoop11:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		System.out.println(new File("conf/kafkaProducer.properties").exists());
//		properties.load(ClassLoader.getSystemResourceAsStream("kafkaProducer.properties"));
//		ProducerConfig config = new ProducerConfig(properties);
		producer = new KafkaProducer<String, String>(properties);
		
//		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
	}

	public void initConsumer(String group)
	{
		Properties properties = new Properties();  
        properties.put("zookeeper.connect", "hadoop2:2181,hadoop1:2181,hadoop3:2181");//声明zk
//        properties.put("zookeeper.connectiontimeout.ms", 1000000L);
        properties.put("group.id", group);
//        properties.put("auto.offset.reset", "smallest");
        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
//        System.out.println("*****");
        
	}
	
	public void fetch(String topic)
	{
		Map<String, Integer> topics = new HashMap<String, Integer>();  
        topics.put(topic, 1); // 一次从主题中获取一个数据  
        Map<String, List<KafkaStream<byte[], byte[]>>>  messageStreams = consumer.createMessageStreams(topics);  
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0); // 获取每次接收到的这个数据
        ConsumerIterator<byte[], byte[]> iterator =  stream.iterator();  
        while(iterator.hasNext())
        {
            String message = new String(iterator.next().message());  
            System.out.println(message);  
            System.out.println("接收到: " + message);  
        }
	}
	
	public void send(String topic,String message) 
	{
		ProducerRecord<String, String> rcd = new ProducerRecord<String, String>(topic, message);
		producer.send(rcd);
	}

	public void send(ProducerRecord<String, String> rcd) 
	{
		producer.send(rcd);
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception 
	{
		KafkaAPI kafka=new KafkaAPI();
		kafka.initProducer();	
		Class.forName( "org.apache.hive.jdbc.HiveDriver");
		Connection con = DriverManager.getConnection("jdbc:hive2://172.16.0.13:10000/graph_data", "", "");
		Statement stmt = con.createStatement();
		stmt.setFetchSize(10000);
		
//		String sql ="select * from dw.update_company";
		
		String sql = "select * from dw.update_company where province='53'";
		ResultSet res = stmt.executeQuery(sql);
		int cnt=0;
		while(res.next())
		{
			String companyName=res.getString(1);
//			String provinceName=res.getString(2);
			String provinceCode=res.getString(3);
//			JSONObject json=new JSONObject();
//			json.put("companyName", companyName);
//			json.put("provinceName", provinceName);
//			json.put("provinceCode", provinceCode);
			
			if(provinceCode!=null && !provinceCode.equals("null") && !provinceCode.equals("\\N"))
			{				
				cnt++;
				String topic="GsSrc"+provinceCode;
				String content=companyName;
				kafka.send(topic, content);
//				System.out.println(content);
				if(cnt%500==0)
				{
					System.out.println("--"+cnt);
				}
			}
		}
		kafka.producer.close();
//		kafka.initConsumer("test");
//		kafka.fetch("GsSrc23");
	}
}