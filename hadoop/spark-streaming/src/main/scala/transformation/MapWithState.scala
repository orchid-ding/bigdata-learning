package transformation

import com.sun.java.swing.plaf.gtk.GTKConstants.StateType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.{MapWithStateDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}

object MapWithState {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    // conf
    val conf = new SparkConf().setMaster("local[2]").setAppName("WorkCount")

    val sc = new SparkContext(conf)

    val scc = new StreamingContext(sc, Seconds(2))

    scc.checkpoint("./data/checkpoint")

    val initialRDD = sc.parallelize(List(("dummy", 100L), ("source", 32L)))

    // socket
    val receiverInputDStream: ReceiverInputDStream[String] = scc.socketTextStream("localhost", 8888)

    val stateSpecFunction = StateSpec.function((currentTime: Time, key: String, value: Option[Int], state: State[Long]) => {
      val sum = value.sum.toLong + state.getOption().getOrElse(0L)
      val output = (key,sum)
      if(!state.isTimingOut()){
        state.update(sum)
      }
      Some(output)
    }).initialState(initialRDD).numPartitions(2).timeout(Seconds(30))

    var result: MapWithStateDStream[String, Int, Long, (String, Long)] = receiverInputDStream.flatMap(_.split(",")).map((_, 1)).mapWithState(stateSpecFunction)

    result.print()

    scc.start()
    scc.awaitTermination()
    scc.stop()

  }

}
