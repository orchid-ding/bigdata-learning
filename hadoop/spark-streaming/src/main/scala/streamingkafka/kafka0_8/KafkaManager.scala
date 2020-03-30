//package streamingkafka.kafka0_8
//
//import kafka.common.TopicAndPartition
//import kafka.message.MessageAndMetadata
//import kafka.serializer.Decoder
//import org.apache.spark.rdd.RDD
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.api.java.{JavaPairInputDStream, JavaStreamingContext}
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.KafkaCluster.{Err, LeaderOffset}
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
//import java.util.{Map => JMap, Set => JSet}
//import scala.reflect.ClassTag
//import scala.collection.JavaConverters._
//
//import scala.reflect.ClassTag
//
//
///**
// * 自定义kafkaManager
// */
//class KafkaManager(val kafkaParam:Map[String,String]) extends Serializable {
//
//  private val kafkaCluster = new KafkaCluster(kafkaParam)
//
//  def createDirectStream[
//    K: ClassTag,
//    V: ClassTag,
//    KD <: Decoder[K]: ClassTag,
//    VD <: Decoder[V]: ClassTag] (
//                                  ssc: StreamingContext,
//                                  kafkaParams: Map[String, String],
//                                  topics: Set[String]
//                                ): InputDStream[(K, V)] = {
//    val groupId = kafkaParams.get("group.id").get
//
//    /**
//     * 获取consumerOffsets之前先根据实际情况更新offsets
//     */
//    setOrUpdateOffsets(topics,groupId)
//
//    var message = {
//      val partitionsE = kafkaCluster.getPartitions(topics)
//      if(partitionsE.isLeft){
//        throw new Exception(s"get kafka partitions failed: ${partitionsE.left.get}")
//      }
//      var partitions = partitionsE.right.get
//      val consumerOffsetsE: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(groupId, partitions)
//      if(consumerOffsetsE.isLeft){
//        throw new Exception(s"get kafka consumer offsets failed : ${consumerOffsetsE.left.get}")
//      }
//      val consumerOffsets = consumerOffsetsE.right.get
//
//      KafkaUtils.createDirectStream[K,V,KD,VD,(K,V)](
//        ssc,kafkaParams,consumerOffsets,(mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
//      )
//    }
//
//    message
//  }
//
//
//
//  def createDirectStream[K, V, KD <: Decoder[K], VD <: Decoder[V]](
//                                                                    jssc: JavaStreamingContext,
//                                                                    keyClass: Class[K],
//                                                                    valueClass: Class[V],
//                                                                    keyDecoderClass: Class[KD],
//                                                                    valueDecoderClass: Class[VD],
//                                                                    kafkaParams: JMap[String,String],
//                                                                    topics: JSet[String]
//                                                                  ): JavaPairInputDStream[K, V] = {
//    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
//    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
//    implicit val keyDecoderCmt: ClassTag[KD] = ClassTag(keyDecoderClass)
//    implicit val valueDecoderCmt: ClassTag[VD] = ClassTag(valueDecoderClass)
//
//    createDirectStream[K, V, KD, VD](jssc.ssc,  Map(kafkaParams.asScala.toSeq: _*),
//      Set(topics.asScala.toSeq: _*));
//  }
//
//  /**
//   * 2. 根据消费情况，更新实际的offset
//   * @param topics
//   * @param groupId
//   */
//  def setOrUpdateOffsets(topics:Set[String], groupId:String):Unit={
//    var hasConsumed = true
//    val partitionsE: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(topics)
//
//    if(partitionsE.isLeft){
//      throw new Exception(s"get kafka partitions failed : ${partitionsE.left.get} ")
//    }
//    val partitions = partitionsE.right.get
//    val consumerOffsetsE: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(groupId, partitions)
//    if (consumerOffsetsE.isLeft) hasConsumed = false
//
//    if(hasConsumed){
//      /**
//       * 如果streaming程序执行的时候出现kafka.common.OffsetOutOfRangeException，
//       * 说明zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
//       * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
//       * 如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时,
//       * 这时把consumerOffsets更新为earliestLeaderOffsets
//       */
//      val earliestLeaderOffsetsE = kafkaCluster.getEarliestLeaderOffsets(partitions)
//      if(earliestLeaderOffsetsE.isLeft){
//       throw new Exception(s"get earliest leads offsets failed:${earliestLeaderOffsetsE.left.get}")
//      }
//
//      var earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
//      var consumerOffsets = consumerOffsetsE.right.get
//
//      /**
//       * 可能只是存在部分分区consumerOffsets过时，所以只更新过时分区的consumerOffsets为earliestLeaderOffsets
//       */
//      var offsets : Map[TopicAndPartition,Long] = Map()
//      consumerOffsets.foreach({
//        case (tp,n)=>
//          val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
//          if(n < earliestLeaderOffset){
//            println("consumer group:" + groupId + ",topic:" + tp.topic + ",partition:" + tp.partition +
//              " offsets已经过时，更新为" + earliestLeaderOffset)
//            offsets += (tp -> earliestLeaderOffset)
//          }
//      })
//      if (!offsets.isEmpty) {
//        kafkaCluster.setConsumerOffsets(groupId, offsets)
//      }
//    }else{
//      /**
//       * 没有消费过数据
//       * 1. auto.offset.reset
//       * default largest
//       *    What to do when there is no initial offset in ZooKeeper or if an offset is out of range:
//       * * smallest : automatically reset the offset to the smallest offset
//       * * largest : automatically reset the offset to the largest offset
//       * * anything else: throw exception to the consumer
//       */
//      val reset = kafkaParam.get("auto.offset.reset").map(_.toLowerCase)
//      var leaderOffsets:Map[TopicAndPartition,LeaderOffset] = null
//
//      if(reset == Some("smallest")){
//        // if is smallest ,get earliest leader offsets
//        val earliestLeaderOffsets = kafkaCluster.getEarliestLeaderOffsets(partitions)
//        if(earliestLeaderOffsets.isLeft){
//          throw new Exception(s"get earliest leader offsets failed: ${earliestLeaderOffsets.left.get}")
//        }
//        leaderOffsets = earliestLeaderOffsets.right.get
//      }else{
//        // else get latest leader offsets
//        val latestLeadOffsets = kafkaCluster.getLatestLeaderOffsets(partitions)
//        if(latestLeadOffsets.isLeft){
//          throw new Exception(s"get latest leader offsets failed:${latestLeadOffsets.left.get}")
//        }
//        leaderOffsets = latestLeadOffsets.right.get
//      }
//      val offsets = leaderOffsets.map{case (tp,offset)=>(tp,offset.offset)}
//      kafkaCluster.setConsumerOffsets(groupId,offsets)
//
//    }
//  }
//
//  /**
//   * update zookeeper offset
//   */
//
//  def updateZookeeperOffsets[K,V](kafkaParam:Map[String,Object],rdd:RDD[(K,V)]): Unit ={
//    val groupId = kafkaParam.get("group.id").get.asInstanceOf[String]
//    val offsetsLists = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//    offsetsLists.foreach(offsets=>{
//      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
//      val value = kafkaCluster.setConsumerOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)))
//      if(value.isLeft){
//        throw new Exception(s"Error updating the offset to kafka cluster : ${value.left.get}")
//      }
//
//    })
//  }
//}
