# SparkAndKafka
## UserKafkaProducer.java
生产者代码。往kafka某一topic生产数据可以在虚拟机中以kafka命令方式进行，也可以写成Java代码连接虚拟机中的kafka。这里写了一个线程类，其中设置broker、topic等属性，连接CentOS中的kafka，生成形如“a1，b2，c3，d4，e5”的String字符串，abcde位置固定，数字位置固定且为随机数。while死循环往topic中不断生产数据。
## UserKafkaConsumer.java
消费者代码。这是对应UserKafkaProducer.java的消费数据的代码，也是线程类，设置broker、topic等属性，然后打印kafka某topic中的数据。
## UserKafkaMain.java
生产者和消费者代码的主类。设置topic，执行上面两个线程。
## SparkStreamingKafka.java
    1）总体实现功能：SparkStreaming流计算从kafka某一topic实时获取数据，实现wordcount，并打印。
      （代码有很多注释，这里做简单介绍）
    2）kafkaConf方法用以设置需要连接的kafka的相关参数；
    3）kafkaOffset方法用以设置kafka的offset；
    4）sparkStreamingConf方法用以配置Spark参数，得到JavaStreamingContext ssc；
       在kafkaMethod方法中，根据之前得到的参数获得JavaInputDStream类型的数据流lines。
       lines是最关键的流数据，可以用一些Spark算子对该流数据进行各种转换。
    5）sparkStreamingMethod方法使用了flatmap、map和reduceByKey算子对lines数据操作，
       得到了counts，counts也是RDD数据，格式为键值对，键是字符串数据，值是字符串出现次数。也就是实现了WordCount。
    6）JavaPairDStreamMethod方法与sparkStreamingMethod方法一样，不过加了一个自己写的select方法，该方法两个参数，String str和String[] params，
       主要实现对字符串的截取，例如，str=“a1，b2，c3，d4，e5”，params={1,3,5}，截取后的字符串是“a1，c3，e5”。
       相当于对数据库中一条记录的某几个字段的字段值进行提取，这是demo代码，所以写的很弱鸡（-_-）
    7）JavaDStreamPrint方法用到了foreach算子，对每个RDD的数据进行打印，打印字符串及其次数。
       打印也可以用自带的print方法，可以每隔一秒打印时间戳，main方法里也写了。
## 相关知识：
sparkstreaming读取kafka有两种方式，我这里用到了Direct方式，具体特点、区别、优劣不展开写，给两篇博客。
* [sparkstreaming读取kafka的两种方式](https://blog.csdn.net/gongpulin/article/details/77619771)
* [spark消费kafka的两种方式](https://blog.csdn.net/woloqun/article/details/80635304)
## 实现过程及截图
* 假设字段及数值如下
![图1](https://github.com/superxinxin/SparkAndKafka/blob/master/Images/1.PNG) 
* 首先执行UserKafkaMain，启动UserKafkaProducer线程生产数据
![图2](https://github.com/superxinxin/SparkAndKafka/blob/master/Images/2.png) 
* 然后在虚拟机的kafka中消费数据
![图3](https://github.com/superxinxin/SparkAndKafka/blob/master/Images/3.png) 
* 设置SparkStreamingKafka所选字段参数
![图4](https://github.com/superxinxin/SparkAndKafka/blob/master/Images/4.png) 
* 运行SparkStreamingKafka，选取任意字段组成数据并统计其出现次数
![图5](https://github.com/superxinxin/SparkAndKafka/blob/master/Images/5.png) 
