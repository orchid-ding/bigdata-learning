//package streamingkafka.kafka0_8
//
//import kafka.serializer.StringDecoder
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object WordCount {
//
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    val conf = new SparkConf()
//        .setMaster("local[2]").setAppName("WordCount_kafka_0.8")
//    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//    val ssc = new StreamingContext(conf, Seconds(10))
//    val brokers = "node01:9092,node02:9092,node03:9092"
//    val topic = "class5"
//    val groupId = "_consumer_group_class5_ds"
//
//    val kafkaParams = Map(
//      "metadata.broker.list" -> brokers,
//      "group.id" -> groupId,
//    "enable.auto.commit"->"false")
//
//    /**
//     * 1、关键步骤一：设置监听器，帮我们完成偏移量的提交
//     */
//    ssc.addStreamingListener(new KafkaStreamingListener(kafkaParams))
//
//    /**
//     * 2、关键步骤二： 创建对象，然后通过这个对象获取到上次的偏移量，然后获取到数据流
//     */
//    val kafkaManager = new KafkaManager(kafkaParams)
//
//    /**
//     * 3、 获取到流，这个流里面是offset的信息的
//     */
//    val dStream: InputDStream[(String, String)] = kafkaManager.createDirectStream[String, String, StringDecoder, StringDecoder](
//      ssc,
//      kafkaParams,
//      topic.split(",").toSet
//    )
//
//    dStream.map(_._2).flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).print()
//
//    ssc.start()
//    ssc.awaitTermination()
//    ssc.stop()
//
//
//
//  }
//}
