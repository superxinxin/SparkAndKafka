package com.nanyou.demo.sparkAndKafka3;

public class UserkafkaMain
{
	public static void main(String[] args)
    {
        UserKafkaProducer producerThread = new UserKafkaProducer("mytopic1");
        producerThread.start();
      // 执行SparkStreamingKafka时，将下面两句注释掉，只需要生产数据，SparkStreaming统计次数并打印即可，无需消费者打印。
      //  UserKafkaConsumer consumerThread = new UserKafkaConsumer("mytopic6");
      //  consumerThread.start();
    }
}
