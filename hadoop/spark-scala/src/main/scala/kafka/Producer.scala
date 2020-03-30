package kafka

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.log4j.BasicConfigurator


object Producer {

  def main(args: Array[String]): Unit = {

    BasicConfigurator.configure()

    val props:Properties = new Properties()

    //kafka集群地址
    //kafka集群地址
    props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    //acks它代表消息确认机制
    props.put("acks", "all")
    //重试的次数
    props.put("retries", "0")
    //批处理数据的大小，每次写入多少数据到topic
    props.put("batch.size", "16384")
    //可以延长多久发送数据
    props.put("linger.ms", "1")
    //缓冲区的大小
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("partitioner.class", "kafka.MyPartitions")

    val producer:Producer[String,String] =  new KafkaProducer[String,String](props)

    for (i <- 1 until 10000){

      producer.send(new ProducerRecord[String, String]("test",  "hello-kafka1-1" + i))
    }
//    producer.send(new ProducerRecord[String, String]("kfly_music", Integer.toString(i), "hello-kafka1-" + i))
//    for (i <- 0 until  10) {
//      //这里需要三个参数，第一个：topic的名称，第二个参数：表示消息的key,第三个参数：消息具体内容
//      producer.send(new ProducerRecord[String, String]("kfly_music", Integer.toString(i), "hello-kafka1-" + i))
//    }

  }





}
