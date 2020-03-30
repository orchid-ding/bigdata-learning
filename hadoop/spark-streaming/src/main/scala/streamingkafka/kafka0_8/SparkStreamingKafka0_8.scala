//package streamingkafka.kafka0_8
//
//import kafka.serializer.StringDecoder
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object SparkStreamingKafka0_8 {
//
//  def main(args: Array[String]): Unit = {
//
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkStream0_8_Kafka")
//
//    val sc = new StreamingContext(conf, Seconds(5))
//
//    val kafkaParam = Map[String, String](
//      "metadata.broker.list" -> "node01:9092,node02:9092,node03:9092",
//      "group.id" -> "orchid.group")
//
//    val topics = "orchid".split(",").toSet
//
//    val dStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](sc, kafkaParam, topics)
//
//    val result = dStream.map(_._2).flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)
//
//    result.print()
//
//    sc.start()
//    sc.awaitTermination()
//    sc.stop()
//  }
//}
