package streamingkafka.kafka0_10

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, OffsetRange}
import org.apache.spark.streaming.scheduler.{StreamInputInfo, StreamingListener, StreamingListenerBatchCompleted, StreamingListenerBatchSubmitted}

class KafkaOffsetListener(var stream:InputDStream[ConsumerRecord[String,String]]) extends StreamingListener{

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    var isHasErr = false
    val outputOperationInfos = batchSubmitted.batchInfo.outputOperationInfos

    outputOperationInfos.foreach(info=>{
      if(!"None".equals(info._2.failureReason.toString)){
        isHasErr = true
      }
    })

    if(! isHasErr){
      val streamIdToInputInfo = batchSubmitted.batchInfo.streamIdToInputInfo

      var offsetRangesTmp: List[OffsetRange] = null;
      var offsetRanges: Array[OffsetRange] = null;

      streamIdToInputInfo.foreach(info=>{
        val option = info._2.metadata.get("offsets")
        if(! option.isEmpty){
          try{
            offsetRangesTmp = option.get.asInstanceOf[List[OffsetRange]]
            offsetRanges = offsetRangesTmp.toSet.toArray
          }catch {
            case e: Exception => println(e)
          }
        }
      })

      if(offsetRanges != null){
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    }
  }
}
