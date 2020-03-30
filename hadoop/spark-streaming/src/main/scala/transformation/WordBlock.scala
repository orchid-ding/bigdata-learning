package transformation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object WordBlock {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordBlock")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("./data/checkpoint")
    val socketDStream = ssc.socketTextStream("localhost", 8888)

    val block = sc.parallelize(List("!" -> true, "#" -> true))

    val broadBlock = sc.broadcast(block.collect())

    val result = socketDStream.flatMap(_.split(",")).map((_, 1)).transform(rdd => {
      val filterRDD = rdd.sparkContext.parallelize(broadBlock.value)
      val value: RDD[(String, (Int, Option[Boolean]))] = rdd.leftOuterJoin(filterRDD)
      value.filter(_._2._2.isEmpty)
    }).map(_._1).map((_, 1)).reduceByKey(_ + _)

    result.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
