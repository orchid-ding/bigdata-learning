package spark.sql.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object DataFromMysql {

  def main(args: Array[String]): Unit = {

    val conf:SparkConf = new SparkConf().setAppName("MysqlSpark").setMaster("local[2]")

    val spark:SparkSession = SparkSession.builder().config(conf).getOrCreate()

    
    //3、读取mysql表的数据
    //3.1 指定mysql连接地址
    val url="jdbc:mysql://node02:3306/kfly"
    //3.2 指定要加载的表名
    val tableName="person"
    // 3.3 配置连接数据库的相关属性
    val properties = new Properties()

    //用户名
    properties.setProperty("user","root")
    //密码
    properties.setProperty("password","123456")

    val mysqlDF = spark.read.jdbc(url,tableName,properties)

    mysqlDF.printSchema()

    //展示数据
    mysqlDF.show()

    mysqlDF.createTempView("user")

    spark.sql("select * from user").show()


    mysqlDF.write.mode("overwrite").jdbc(url,"user1",properties)


  }
}
