package spark.sql.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HiveSupport {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession
                    .builder()
                    .master("local[2]")
                    .appName("SparkSqlHiveSupport")
                    .enableHiveSupport()
                    .getOrCreate()


//    spark.sql("create table person1(id int,name string,age int) row format delimited fields terminated by ','")

    spark.sql("load data local inpath './doc/person.txt' into table person1")


    spark.sql("select * from person1").show()
  }
}
