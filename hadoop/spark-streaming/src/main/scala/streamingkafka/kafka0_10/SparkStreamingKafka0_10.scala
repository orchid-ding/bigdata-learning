package streamingkafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 高版本本身数据不丢失
 */
object SparkStreamingKafka0_10 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkStream0_8_Kafka")

    /**
     * 使用新的Kafka直接流API时，将从每个Kafka分区读取数据的最大速率（每秒的记录数）
     */
    conf.set("spark.streaming.kafka.maxRatePerPartition","5")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new StreamingContext(conf, Seconds(5))

    val kafkaParam = Map[String, Object](
      "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      "group.id" -> "orchid.group",

      /**
       * spark streaming 消费的kafka一条数据可以有多大
       * 默认是1M，生产时避免单条数据超过1M设置成10M
       */
      "fetch.message.max.bytes"-> "209715200",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> "false"
    )


    val topics = "orchid".split(",").toSet

    val dStream = KafkaUtils.createDirectStream[String,String](
                                  sc,
                                  LocationStrategies.PreferConsistent,
                                ConsumerStrategies.Subscribe[String,String](topics,kafkaParam)
                                )

    dStream.foreachRDD(rdd=>{
      val newRDD = rdd.map(_.value())
      newRDD.foreach(record=>{
        println("recode -> " + record)
      })
      val offsetRanges:Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      dStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      println("offset-> ")
      offsetRanges.foreach(offsetRange=>{
        print(offsetRange.toString())
      })
    })


    sc.start()
    sc.awaitTermination()
    sc.stop()
  }
}
