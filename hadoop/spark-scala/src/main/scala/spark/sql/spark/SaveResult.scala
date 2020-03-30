package spark.sql.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SaveResult {

  def main(args: Array[String]): Unit = {
    //1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("SaveResult").setMaster("local[2]")

    //2、创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


    val dataFrame = spark.read.json("./doc/score.json")

//    dataFrame.select("name").show()

    dataFrame.createTempView("score")

//    spark.sql("select * from score where classNum =10").show()

//    dataFrame.write.parquet("./doc/sql/parquet")

//    dataFrame.write.csv("./doc/sql/csv")

    dataFrame.select("name").write.text("./doc/sql/text")
    //todo: 5.7  按照单个字段进行分区 分目录进行存储
    dataFrame.write.partitionBy("classNum").json("./data/partitions")

    //todo: 5.8  按照多个字段进行分区 分目录进行存储
    dataFrame.write.partitionBy("classNum","name").json("./data/numPartitions")
  }
}
