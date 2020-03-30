package output

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CheckPoint {

  def main(args: Array[String]): Unit = {

    print(1)
    Logger.getLogger("org").setLevel(Level.ERROR)
    val ssc = StreamingContext.getOrCreate("./data/checkpoint1",functionCreateStreamingContext)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }


  def functionCreateStreamingContext() : StreamingContext = {
    val conf:SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val sc:SparkContext = new SparkContext(conf)

    val scc:StreamingContext = new StreamingContext(sc,Seconds(5))
    scc.checkpoint("./data/checkpoint1")

    val dstream = scc.socketTextStream("localhost",8888)

    val value: DStream[(String, Int)] = dstream.flatMap(_.split(",")).map((_, 1))
      .updateStateByKey((values: Seq[Int], state: Option[Int]) => {
        val currentCount = values.sum
        val lastCount = state.getOrElse(0)
        Some(currentCount + lastCount)
      })

    value.print()

    scc.start()
    scc.awaitTermination()
    scc.stop()
    scc
  }
}
