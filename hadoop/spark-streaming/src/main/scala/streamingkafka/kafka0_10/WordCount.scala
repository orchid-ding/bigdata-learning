//package streamingkafka.kafka0_10
//
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//import scala.collection.mutable.{HashMap => MHashMap, Map => MMap}
//
//
//object WordCount {
//
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
//    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
//
//    val ssc = new StreamingContext(conf, Seconds(10))
//
//    val brokers = "node01:9092,node02:9092,node03:9092"
//    val groupId = "group_consumer_topic415"
//    val topic = "class5".split(",").toSet
//
//
//    val kafkaParams = Map[String,Object](
//      "bootstrap.servers" -> brokers,
//      "group.id" -> groupId,
//        "enable.auto.commit"->"false",
//      "fetch.message.max.bytes" -> "209715200",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer]
//    )
//
//    val dStream = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe[String,String](topic, kafkaParams)
//    )
//    ssc.addStreamingListener(new KafkaOffsetListener(dStream))
//
//    dStream.map(_.value()).flatMap(_.split("，")).map((_,1)).reduceByKey(_+_).print()
//
//    ssc.start()
//    ssc.awaitTermination()
//    ssc.stop()
//
//  }
//
//}
