package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

object SpackScalaPV {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("SparkPV").setMaster("local[2]")

    val sc:SparkContext = new SparkContext(conf)

    val data:RDD[String] = sc.textFile("/Users/dingchuangshi/IdeaProjects/java/hadoop/spark-scala/doc/users.dat")

    val filterRDD:RDD[String] = data.filter(x=>x.split(" ").length>10)

    val filterRDD2:RDD[String] = filterRDD.filter(!_.split(" ")(10).equals("\"-\""))

    val urlAndOne:RDD[(String,Int)] = filterRDD.map(_.split(" ")(10)).map((_,1))

    val reduceData:RDD[(String,Int)] = urlAndOne.reduceByKey(_+_)

    val sorted:RDD[(String,Int)] = reduceData.sortBy(_._2,false)

    sorted.take(5).foreach(println)
//    sorted.saveAsTextFile("/Users/dingchuangshi/IdeaProjects/java/hadoop/spark-scala/doc/users.dat-out")
//
//    sc.stop()

  }
}
