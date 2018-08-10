package com.nanyou.demo.sparkAndKafka3;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import org.apache.spark.api.java.function.VoidFunction;

public class SparkSteamingKafka
{
	public static void main(String[] args) throws InterruptedException
	{
		//指定kafka中的topic
		String topics = "mytopic1";
		//获得topics集合，此处只有一个topic：mytopic6
		Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		//设置kafka参数
		Map<String, Object> kafkaParams = kafkaConf(topics);
		//设置offset参数
		HashMap offsets = kafkaOffset(topics);
		kafkaParams.put("auto.offset.reset", "earliest");
		//设置sparkStreaming参数
		JavaStreamingContext ssc = sparkStreamingConf();
		//通过KafkaMethod获得kafka的DirectStream数据，kafka相关参数由kafkaParams指定
		JavaInputDStream<ConsumerRecord<Object, Object>> lines = kafkaMethod(ssc, topicsSet, kafkaParams, offsets);
		//通过sparkStreaming方法获得字符串及其出现次数
		//JavaPairDStream<String, Integer> counts = sparkStreamingMethod(lines);
		//counts.print();	
		//设置任意参数。
		String[] params = args;
		//调用JavaPairDStreamMethod方法，获取params中指定字段的键值对数据。
		JavaPairDStream<String, Integer> counts1 = JavaPairDStreamMethod(lines, params);
//		counts1.print();
		JavaDStreamPrint(counts1);
		// 可以打印所有信息，看下ConsumerRecord的结构
//		 lines.foreachRDD(rdd -> {
//		 rdd.foreach(x -> {
//		 System.out.println(x);
//		 });
//		 });
		ssc.start();
		ssc.awaitTermination();
		ssc.close();
	}
	public static String select(String str, String[] params)
	{
		int[] nums = new int[params.length];
		for(int i=0; i<params.length; i++)
		{
			nums[i] = Integer.parseInt(params[i])-1;
		}
		String[] temp = str.split(",");
		String res = "";
		for(int i=0; i<nums.length; i++)
		{
			res += temp[nums[i]]+",";
		}
		res = res.substring(0, res.length()-1);
		return res;
	}
	public static Map<String, Object> kafkaConf(String topics)
	{
		String brokers = "192.168.190.125:9092";
		Map<String, Object> kafkaParams = new HashMap<>();
		// kafka相关参数，必要！缺了会报错
		kafkaParams.put("metadata.broker.list", brokers);
		kafkaParams.put("bootstrap.servers", brokers);
		kafkaParams.put("group.id", "console-consumer-30086");
		kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return kafkaParams;
	}
	public static HashMap kafkaOffset(String topics)
	{
		// Topic分区 也可以通过配置项实现
		// 如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
		// earliest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
		// latest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
		// none topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
		// kafkaParams.put("auto.offset.reset", "latest");
		// kafkaParams.put("enable.auto.commit",false);
		HashMap offsets = new HashMap<>();
		offsets.put(new TopicPartition(topics, 0), 2L);
		return offsets;
	}
	public static JavaStreamingContext sparkStreamingConf()
	{
		//注意本地调试，master必须为local[n],n>1,表示一个线程接收数据，n-1个线程处理数据
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("streaming word count");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));
		return ssc;
	}
	// 通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
	public static JavaInputDStream<ConsumerRecord<Object, Object>> kafkaMethod
	(JavaStreamingContext ssc, Collection<String> topicsSet, Map<String, Object> kafkaParams, HashMap offsets)
	{
		JavaInputDStream<ConsumerRecord<Object, Object>> lines = KafkaUtils.createDirectStream(ssc,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicsSet, kafkaParams, offsets));
		return lines;
	}
	public static JavaPairDStream<String, Integer> sparkStreamingMethod(JavaInputDStream<ConsumerRecord<Object, Object>> lines)
	{
		//我们将 lines 这个DStream转成counts对象，其实作用于lines上的flatMap算子，
		//会施加于lines中的每个RDD上，并生成新的对应的RDD，而这些新生成的RDD对象就组成了counts这个DStream对象。
		//底层的RDD转换仍然是由Spark引擎来计算。DStream的算子将这些细节隐藏了起来，并为开发者提供了更为方便的高级API。
		JavaPairDStream<String, Integer> counts =
		lines.flatMap(x -> Arrays.asList(x.value().toString().split(" ")).iterator())
		.mapToPair(x -> new Tuple2<String, Integer>(x, 1)).reduceByKey((x, y) -> x + y);
		return counts;
	}
	public static JavaPairDStream<String, Integer> JavaPairDStreamMethod(JavaInputDStream<ConsumerRecord<Object, Object>> lines, String[] params)
	{
		//select方法用于修改每条数据字符串，截取指定参数params的字符串，flatMap算子将其转换成JavaDStream类型。
		JavaDStream<String> counts2 = 
				lines.flatMap(x -> Arrays.asList(select(x.value().toString(), params)).iterator());
		JavaPairDStream<String, Integer> counts3 = 
				counts2.mapToPair(x -> new Tuple2<String, Integer>(x, 1)).reduceByKey((x, y) -> x + y);
		return counts3;
	}
	public static void JavaDStreamPrint(JavaPairDStream<String, Integer> counts1)
	{
		counts1.foreachRDD(
                (VoidFunction<JavaPairRDD<String, Integer>>) values -> {
                    values.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                        @Override
                        public void call(Tuple2<String, Integer> tuple) throws Exception {
                            System.out.println("counts3:" + tuple._1() 
                            +" - "+ tuple._2());
                        }
                    });
                });
	}
}