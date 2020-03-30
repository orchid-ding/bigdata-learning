package spark.partitions

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{Accumulator, Partition, Partitioner, SparkConf, SparkContext}
import sun.security.util.Length

object CustomPartitions {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("TextPartition").setMaster("local[2]")

    val sc: SparkContext = new SparkContext(conf)

    val rdd:RDD[String] = sc.parallelize(List("hadoop","idea","name","hadoop","spark","map","map","age","student","123456"))

    val rd2:RDD[(String,Int)] = rdd.map((_,1))

    rd2.reduceByKey(_+_).partitionBy(new CustomPartition(3)).saveAsTextFile("./doc/data")
    0

  }

  class CustomPartition(val num:Int) extends Partitioner{

    override def numPartitions: Int = {
      num
    }

    override def getPartition(key: Any): Int = {
      val length:Int = key.toString.length
      length match {
        case 4 => 0
        case 5 => 1
        case 6 => 2
        case _ => 0
      }
    }
  }
}


