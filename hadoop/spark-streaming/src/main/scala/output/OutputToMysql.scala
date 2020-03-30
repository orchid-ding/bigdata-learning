package output

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OutputToMysql {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[2]").setAppName("WorkCountToMysql")

    val sc = new StreamingContext(conf, Seconds(2))

    sc.checkpoint("./data/checkpoint1")

    val dStream = sc.socketTextStream("localhost",8888)

    val resultDStream = dStream.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)

    /**
     * output to mysql 1
     */
//    resultDStream.foreachRDD(rdd=>{
//      rdd.foreach(recode=>{
//        val conn = getConnByJDBC()
//        // executed at the worker
//        val ps: PreparedStatement = conn.prepareStatement("insert into kfly.stu values(?,?)")
//        ps.setInt(1, recode._2)
//        ps.setString(2, recode._1)
//        ps.execute
//        ps.close()
//      })
//    })


    resultDStream.foreachRDD((rdd,time)=>{
      rdd.foreachPartition(partition=>{
        val conn = ConnectionPool.getConn
        conn.setAutoCommit(false)
        val statement = conn.prepareStatement("insert into kfly.stu values(?,?)")

        partition.foreach{
          case (word:String,count:Int) => {
//            statement.setLong(1,time.milliseconds)
            statement.setString(2,word)
            statement.setInt(1,count)
            statement.addBatch()
          }
        }
        statement.executeBatch()
        statement.close()
        conn.commit()
        conn.setAutoCommit(true)
        ConnectionPool.returnConn(conn)
      })
    })
    sc.start()
    sc.awaitTermination()
    sc.stop()
  }

}
