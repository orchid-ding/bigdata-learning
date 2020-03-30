import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowsOperator {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[2]").setAppName("WorkCount")
    val sc = new StreamingContext(conf, Seconds(1))

    sc.checkpoint("./data/checkpoint")

    val socketDStream = sc.socketTextStream("localhost", 8888)

//    val dStream: DStream[String] = socketDStream.window(Seconds(4), Seconds(2))
//
//    val result = socketDStream.flatMap(_.split(",")).map((_, 1)).reduceByKey(_+_)

    val result = socketDStream.flatMap(_.split(",")).map((_, 1)).reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(4), Seconds(2))
    result.print()

    sc.start()
    sc.awaitTermination()
    sc.stop()
  }
}
