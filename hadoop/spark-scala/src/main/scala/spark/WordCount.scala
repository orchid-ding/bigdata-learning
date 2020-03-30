package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

      new SparkContext(
        new SparkConf()
          .setAppName("WorkCount")
          .setMaster("local[2]")
      )
        .textFile(args(0))
        .flatMap(_.split(" "))
        .map(x=>(x,1))
        .reduceByKey(_+_)
        .sortBy(x=>x._2,false)
        .saveAsTextFile(args(1))
  }

}
