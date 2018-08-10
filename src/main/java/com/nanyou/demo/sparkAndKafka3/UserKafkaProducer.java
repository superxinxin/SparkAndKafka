package com.nanyou.demo.sparkAndKafka3;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
public class UserKafkaProducer extends Thread
{
	private final KafkaProducer<Integer, String> producer;
	private final String topic;
	private final Properties props = new Properties();
	public UserKafkaProducer(String topic)
	{
		props.put("metadata.broker.list", "192.168.190.125:9092");
		props.put("bootstrap.servers", "192.168.190.125:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<Integer, String>(props);
		this.topic = topic;
	}
	@Override
	public void run()
	{
		//int messageNo = 1;
		while (true)
		{
			String[] str = new String[]{"a", "b", "c", "d", "e", "f", "g"};
			String s = "";
			for(int i=0; i<str.length; i++)
			{
				int temp = (int)(1+(Math.random()*4));
				s = s + str[i]+String.valueOf(temp)+",";
			}
			String res = s.substring(0,  s.length()-1);
		//	String messageStr = new String("Message_" + messageNo);
			String messageStr = new String(res);
			System.out.println("Send:" + messageStr);
			producer.send(new ProducerRecord<Integer, String>(topic,messageStr));
			//messageNo++;
			try
			{
				sleep(3000);
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}
}