import java.util.Comparator
import java.{lang, util}
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

import com.sun.tools.corba.se.idl.constExpr.Times
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object FlinkOrder {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironment()

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setRestartStrategy(RestartStrategies
      // 重启三次，
      .fixedDelayRestart(3, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)))

    env.enableCheckpointing(1000)
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    val socketStreaming = env.socketTextStream("localhost", 8088)

    socketStreaming
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[String] {
        var currentMaxTimestamp = 0L
        /**
         * 允许乱序时间。
         */
        val maxOutputOfOrderness = 100000L
        override def getCurrentWatermark: Watermark = {
          new Watermark(currentMaxTimestamp -maxOutputOfOrderness)
        }

        override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {
          val dataTimeStr = element.split(",")(0)
          val sdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm")
          sdf.parse(dataTimeStr).getTime
        }
      })
        .timeWindowAll(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
//        .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
        .process(new ProcessWindowFunction[String,(String,String),String,TimeWindow] {
          override def process(key: String, context: ProcessWindowFunction[String, (String, String), String, TimeWindow]#Context, elements: lang.Iterable[String], out: Collector[(String, String)]): Unit = {
            var list = new util.ArrayList[(String,String)]()
            val value = elements.iterator()
            while(value.hasNext){
              val strings = value.next().split(",")
              list.add((strings(0),strings(1)))
            }
            list.sort(new Comparator[(String, String)] {
              override def compare(o1: (String, String), o2: (String, String)): Int = {
                o1._1.compareTo(o2._1)
              }
            })
            list.forEach(new Consumer[(String,String)]{
              override def accept(t: (String, String)): Unit = {
                out.collect(t)
              }
            })
          }
        }){}
        .print()

    env.execute()


  }
}
