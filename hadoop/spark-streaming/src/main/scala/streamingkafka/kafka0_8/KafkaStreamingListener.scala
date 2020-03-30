//package streamingkafka.kafka0_8
//
//import kafka.common.TopicAndPartition
//import org.apache.spark.streaming.kafka.{KafkaCluster, OffsetRange}
//import org.apache.spark.streaming.scheduler.{StreamInputInfo, StreamingListener, StreamingListenerBatchCompleted}
//
//import scala.collection.immutable.{HashMap, List, Map}
//import scala.collection.mutable.{HashMap=>JHashMap,  Map=>JMap}
//
//class KafkaStreamingListener(val kafkaParams:Map[String,String]) extends StreamingListener{
//
//  var kc: KafkaCluster = new KafkaCluster(kafkaParams)
//
//  /**
//   * 当一个SparkStreaing程序运行完了以后，会触发这个方法
//   * 里面方法里面完成偏移量的提交
//   *
//   * @param batchCompleted
//   */
//  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
//    /**
//     * 一个批次里面是有多个task，一般你有几个分区，就会有几个task任务。
//     * 万一，比如有10个task，有8个task运行成功了，2个 task运行失败了。
//     * 但是我们偏移量会被照常提交，那这样的话，会丢数据。
//     * 所以我们要判断一个事，只有所有的task都运行成功了，才提交偏移量。
//     *
//     * 10 task   5task 运行成功  5task运行失败，不让提交偏移量
//     * 会有小量的数据重复，这个是在企业里面95%的场景都是接受的。
//     * 如果是我们的公司，我们公司里面所有的实时的任务都接受有少量的数据重复。但是就是不允许丢失。
//     *
//     * 如果是运行成功的task，是没有失败的原因的（ failureReason 这个字段是空的）
//     * 如果说一个task是失败了，那必行failureReason 这个字段里面有值，会告诉你失败的原因。
//     *
//     */
//
//    // 如果本批次里面有任务失败了，那么就终止偏移量提交
//    val outputOperationInfos = batchCompleted.batchInfo.outputOperationInfos
//      outputOperationInfos.foreach(outputOperationInfo=>{
//        //failureReason不等于None(是scala中的None),说明有异常，不提交偏移量
//        if(!"None".equals(outputOperationInfo._2.failureReason.toString)){
//          return
//        }
//    })
//
//    val batchTime = batchCompleted.batchInfo.batchTime
//
//    /**
//     * topic partitions offsets
//     */
//
//    val offsets = getOffsetsByBatchCompleted(batchCompleted)
//    offsets.foreach(offset=>{
//      val topic:String = offset._1
//      val offsetRange:JMap[Int,Long] = offset._2
//      val groupId:String = kafkaParams.get("group.id").get
//      offsetRange.foreach(partitionDataAndOffset=>{
//        kc.setConsumerOffsets(
//          groupId,
//          Map(
//            TopicAndPartition(topic, partitionDataAndOffset._1)->partitionDataAndOffset._2)
//        )
//      })
//    })
//
//
//
//  }
//
//  /**
//   *
//   * @param batchCompleted
//   * @return  topic、partitions 、 offsets
//   */
//  def getOffsetsByBatchCompleted(batchCompleted: StreamingListenerBatchCompleted):JMap[String,JMap[Int,Long]] = {
//    val map = new JHashMap[String, JMap[Int, Long]]
//    val streamIdToInputInfo:Map[Int,StreamInputInfo] = batchCompleted.batchInfo.streamIdToInputInfo
//    streamIdToInputInfo.foreach(info=>{
//      val option = info._2.metadata.get("offsets")
//      if(!option.isEmpty){
//        val objOffset = option.get
//        if(classOf[List[_]].isAssignableFrom(objOffset.getClass)){
//          val offset:List[OffsetRange] = objOffset.asInstanceOf[List[OffsetRange]]
//          offset.foreach(range=>{
//            if(!map.contains(range.topic)){
//              map += (range.topic->new JHashMap[Int,Long])
//            }
//            map.get(range.topic).get += (range.partition->range.untilOffset)
//          })
//        }
//      }
//    })
//    map
//  }
//
//
//
//
//}
