package top.kfly.init

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.types.Row
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat

/**
 * 初始化商品数据
 */
object GenerateGoodsData {
  def main(args: Array[String]): Unit = {

    val env : ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val fileSource : DataSet[String] = env.readTextFile("/Users/dingchuangshi/Projects/java/hadoop/flink/flink-realtime-mall/doc/kaikeba_goods.csv")

    val lineData = fileSource.flatMap(x=>x.split("\r\n"))

    val objDataSet: DataSet[Row] = lineData.map(x => {
      val pro: Array[String] = x.split("===")
      Row.of(null, pro(1), pro(2), pro(3), pro(4), pro(5), pro(6), pro(7), pro(8), pro(9), pro(10))
    })

    objDataSet.output(JDBCOutputFormat.buildJDBCOutputFormat()
      .setBatchInterval(2)
      .setDBUrl("jdbc:mysql://node02:3306/flink_house?characterEncoding=utf-8")
      .setDrivername("com.mysql.jdbc.Driver")
      .setPassword("123456")
      .setUsername("root")
      .setQuery("insert into kfly_goods(goodsId ,goodsName ,sellingPrice,productPic ,productBrand  ,productfbl  ,productNum ,productUrl ,productFrom,goodsStock ,appraiseNum   ) values(?,?,?,?,?,?,?,?,?,?,?)")
      .finish())

    env.execute()

  }
}
