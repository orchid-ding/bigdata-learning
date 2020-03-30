package kafka

import java.util.Arrays.asList
import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.BasicConfigurator

object Consumer {

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    //准备配置属性
    val props = new Properties()
    //kafka集群地址
    props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    //消费者组id
    props.put("group.id", "test")
    //自动提交偏移量
    props.put("enable.auto.commit", "true")
    //自动提交偏移量的时间间隔
    props.put("auto.commit.interval.ms", "1000")
    //默认是latest
    //earliest: 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
    //latest: 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
    //none : topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
    props.put("auto.offset.reset", "earliest")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new KafkaConsumer[String, String](props)
    //指定消费哪些topic
    consumer.subscribe(asList("kfly_music"))
    while ( {
      true
    }) { //指定每个多久拉取一次数据
      val records = consumer.poll(100)
      import scala.collection.JavaConversions._
      for (record <- records) {
        printf("offset = %d, key = %s, value = %s%n", record.offset, record.key, record.value)
      }
    }
  }
}
