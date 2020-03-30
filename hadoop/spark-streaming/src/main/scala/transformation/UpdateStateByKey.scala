package transformation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKey {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("WorkCount")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("./data/checkpoint")
    val socketStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)

    val result: DStream[(String, Int)] = socketStream
      .flatMap(_.split(",")).map((_, 1))
      .updateStateByKey((values:Seq[Int],state:Option[Int])=>{
        val currentCount = values.sum
        val lastCount = state.getOrElse(0)
        Some(currentCount + lastCount)
      })

    result.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }


}
