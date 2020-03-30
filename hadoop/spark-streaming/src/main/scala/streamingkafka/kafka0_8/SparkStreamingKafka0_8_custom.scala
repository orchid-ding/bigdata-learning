//package streamingkafka.kafka0_8
//
//import kafka.serializer.StringDecoder
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object SparkStreamingKafka0_8_custom {
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
//    val kafkaManager = new KafkaManager(kafkaParam)
//
//    val dStream: InputDStream[(String, String)] =
////      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](sc, kafkaParam, topics)
//        kafkaManager.createDirectStream[String,String,StringDecoder,StringDecoder](sc,kafkaParam,topics)
//
//    dStream.foreachRDD(rdd=>{
////      rdd.map(_._2).flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)
//      rdd.foreachPartition(partitions=>{
//        partitions.foreach(a=>println(a))
//      })
//      kafkaManager.updateZookeeperOffsets(kafkaParam,rdd)
//    })
//
//
//    sc.start()
//    sc.awaitTermination()
//    sc.stop()
//  }
//}
