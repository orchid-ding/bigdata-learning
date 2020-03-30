/**
 * 实时处理kafka的数据
 */
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MainTest {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[3]").setAppName("server")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "5")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    val sc = new SparkContext(conf)
    //配置实时程序入口
    val ssc = new StreamingContext(sc , Seconds(2))

    val brokers = "node01:9092,node02:9092,node03:9092"
    val topics = "gsp"
    val groupId = "gsp"

    val topic_set = topics.split(",").toSet   //消费的topic是以集合的形式

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean),   //手动提交偏移量
      "fetch.message.max.bytes" -> classOf[StringDeserializer]
    )

    //获取数据源头
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,         //洱海说这里传入的是从哪消费的位置
      ConsumerStrategies.Subscribe[String, String](topic_set, kafkaParams)  //消费策略
    )


    stream.map( x => (x.value)).flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).print()

    /*stream.foreachRDD( rdd =>{
      //步骤三：业务逻辑处理
      val newRDD: RDD[String] = rdd.map(_.value())
      newRDD.foreach( line =>{
        println(line)
      })

      //步骤四：提交偏移量信息，把偏移量信息添加到kafka里
      val offsetRanges  = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })*/

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
