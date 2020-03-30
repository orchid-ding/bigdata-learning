package spark.partitions

import java.sql.{Connection, DriverManager}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.glassfish.jersey.server.Broadcaster

object BroadCastScala {


  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("BroadCase").setMaster("local[2]")

    val sc: SparkContext = new SparkContext(conf)

    val accumulator:LongAccumulator = sc.longAccumulator("Count")

    val rdd:RDD[String] = sc.textFile("/Users/dingchuangshi/IdeaProjects/java/hadoop/spark-scala/doc/words.txt")

    val address:Address = new Address("hadoop")

    val words:Broadcast[Address] = sc.broadcast(address)

    val mapRdd:RDD[String] = rdd.flatMap(_.split(" "))

    val resultRdd = mapRdd.filter(_.equals(words.value.name))

    resultRdd.collect().foreach(println)
  }

  case class Address(val name:String){
    val ad:Add = new Add
  }

  case class Add(){
    val name:String = "1"
  }
}
