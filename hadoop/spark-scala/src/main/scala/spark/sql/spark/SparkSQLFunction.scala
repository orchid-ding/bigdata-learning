package spark.sql.spark

import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.spark.sql.SparkSession

object SparkSQLFunction {

  def main(args: Array[String]): Unit = {
    //1、创建SparkSession
    val spark: SparkSession = SparkSession.builder().appName("SparkSQLFunction").master("local[2]").getOrCreate()

    val dataFrame = spark.read.json("./doc/score.json")


    dataFrame.createTempView("score")

    spark.udf.register("low2UP",(x:String)=>x.toUpperCase())

    spark.udf.register("up2low",(x:String)=>x.toLowerCase())

    spark.sql("select * from score").show()

    spark.sql("select low2UP(name) from score").show()

    spark.sql("select up2low(name) from score").show()

  }
}
