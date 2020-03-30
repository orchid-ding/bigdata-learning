package com.travel.programApp

import com.travel.common.{ConfigUtil, Constants}
import com.travel.listener.SparkStreamingListener
import com.travel.utils.{HbaseTools, JsonParse}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object StreamingMaxwellKafka {

  def main(args: Array[String]): Unit = {
    val brokers = ConfigUtil.getConfig(Constants.KAFKA_BOOTSTRAP_SERVERS)
    val topics = Array(Constants.VECHE)
    val conf = new SparkConf().setMaster("local[4]").setAppName("sparkMaxwell")
    val group_id:String = "vech_group"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group_id,
      "auto.offset.reset" -> "earliest",// earliest,latest,和none
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val context: SparkContext = sparkSession.sparkContext
    context.setLogLevel("WARN")
    // val streamingContext = new StreamingContext(conf,Seconds(5))
    //获取streamingContext


    val ssc: StreamingContext =  new StreamingContext(context,Seconds(1))
    val getDataFromKafka: InputDStream[ConsumerRecord[String, String]] = HbaseTools.getStreamingContextFromHBase(ssc,kafkaParams,topics,group_id,"veche")

    getDataFromKafka.foreachRDD(eachRdd =>{
      if(!eachRdd.isEmpty()){
        val catchResult =   Try{
          eachRdd.foreachPartition(eachPartition =>{
            //每个分区获取一次连接
            val conn = HbaseTools.getHbaseConn
            eachPartition.foreach(eachLine =>{
              //获取到每条数据
              val jsonStr: String = eachLine.value()
              //（表名称 ， bean）
              val parse: (String, Any) = JsonParse.parse(jsonStr)
              HbaseTools.saveBusinessDatas(parse._1,parse,conn)
            })
            HbaseTools.closeConn(conn)
          })
        }
        //每个分区更新数据
      /*  eachRdd.foreachPartition(eachPartition =>{
          val list: List[ConsumerRecord[String, String]] = eachPartition.toList
          val finalResult: ConsumerRecord[String, String] = list(list.size - 1)
          val endOffset: Long = finalResult.offset()  //结束offset
          val topic: String = finalResult.topic
          val partition: Int = finalResult.partition
          HbaseTools.saveBatchOffset(group_id,topic,partition+"",endOffset)
        })*/
        //更新offset
        val offsetRanges: Array[OffsetRange] = eachRdd.asInstanceOf[HasOffsetRanges].offsetRanges
        getDataFromKafka.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)  //将offset提交到默认的kafka的topic里面去保存
        for(eachrange <-  offsetRanges){
          val startOffset: Long = eachrange.fromOffset  //起始offset
          val endOffset: Long = eachrange.untilOffset  //结束offset
          val topic: String = eachrange.topic
          val partition: Int = eachrange.partition
          HbaseTools.saveBatchOffset(group_id,topic,partition+"",endOffset)
        }
      }
    })

    ssc.addStreamingListener(new SparkStreamingListener(1))
    ssc.start()
    ssc.awaitTermination()
  }
}
