package top.kfly.syncdata.orders

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala._
import top.kfly.pojo.OrderObj

object IncrementOrder {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setRestartStrategy(
      RestartStrategies
        .fixedDelayRestart(3,10000))

    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.setCheckpointTimeout(6000)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val props = new Properties
    props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    props.put("zookeeper.connect", "node01:2181,node02:2181,node03:2181")
    props.put("group.id", "flinkHouseGroup")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("flink.partition-discovery.interval-millis", "30000")

    val kafkaSource = new FlinkKafkaConsumer011[String]("flink_house",new SimpleStringSchema(),props)
    kafkaSource.setCommitOffsetsOnCheckpoints(true)

    val result: DataStream[String] =env.addSource(kafkaSource)

    val orderResult: DataStream[OrderObj] = result.map(x => {
      val jsonObj: JSONObject = JSON.parseObject(x)
      val database: AnyRef = jsonObj.get("database")
      val table: AnyRef = jsonObj.get("table")
      val `type`: AnyRef = jsonObj.get("type")
      val string: String = jsonObj.get("data").toString
      OrderObj(database.toString,table.toString,`type`.toString,string)
    })
    orderResult.addSink(new HBaseSinkFunction)
    env.execute()



  }
}
