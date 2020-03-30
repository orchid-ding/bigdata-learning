package sqlstreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkSqlAndStreaming {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[2]").setAppName("Sql and streaming")

    val sc = new StreamingContext(conf, Seconds(5))
    
    sc.checkpoint("./data/checkpoint")

    val dStream = sc.socketTextStream("localhost", 8888, StorageLevel.MEMORY_ONLY)


    val value = dStream.flatMap(_.split(","))


    value.foreachRDD(rdd=>{
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()

      import spark.implicits._

      val df = rdd.toDF("word")
      val word = df.createOrReplaceTempView("words")

      spark.sql("select word,count(1) from words group by word").show()
    })

    sc.start()
    sc.awaitTermination()
    sc.stop()
  }
}
