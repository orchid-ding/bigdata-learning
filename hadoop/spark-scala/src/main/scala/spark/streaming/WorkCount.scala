package spark.streaming
import java.util.Date
import java.util.logging.Logger

import com.sun.javafx.binding.Logging
import org.apache.hadoop.hive.ql.session.OperationLog.LoggingLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object WorkCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("StreamingWorkCount")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(5))

    val sscSocket: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)


    val value = sscSocket.flatMap(_.split(",")).map((_, 1))
      .updateStateByKey((values: Seq[Int], state:Option[Int])=>{
        val currentSum = values.sum
        val stateSum = state.getOrElse(0)
        Some(currentSum + stateSum)
      })

    value.print()

    ssc.checkpoint("./data")
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
