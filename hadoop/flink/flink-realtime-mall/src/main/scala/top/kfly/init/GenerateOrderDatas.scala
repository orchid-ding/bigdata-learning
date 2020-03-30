package top.kfly.init

import org.apache.flink.api.scala._
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Date, Random, UUID}

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import org.apache.flink.types.Row
import top.kfly.pojo.Order

object GenerateOrderDatas {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val appendTableSink : JDBCAppendTableSink = JDBCAppendTableSink.builder()
      .setBatchSize(2)
      .setDBUrl("jdbc:mysql://node02:3306/flink_house?characterEncoding=utf-8")
      .setDrivername("com.mysql.jdbc.Driver")
      .setPassword("123456")
      .setUsername("root")
      .setBatchSize(2)
      .setQuery("insert into kfly_orders (orderNo,userId ,goodId ,goodsMoney ,realTotalMoney ,payFrom ,province) values (?,?,?,?,?,?,?)")
      .setParameterTypes(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
      .build()

    val sourceStream : DataStreamSource[Row] = env.addSource(new RichParallelSourceFunction[Row] {
      var isRunning = true

      override def run(ctx: SourceFunction.SourceContext[Row]): Unit = {
        while (isRunning) {
          val order: Order = generateOrder
          ctx.collect(Row.of(order.orderNo, order.userId, order.goodId, order.goodsMoney
            , order.realTotalMoney, order.payFrom, order.province))
          Thread.sleep(1000)
        }
      }

      override def cancel(): Unit = {
        isRunning = false
      }
    })

    appendTableSink.emitDataStream(sourceStream)

    env.execute();



  }


  /**
   * 随机生成订单
   * @return
   */
  def generateOrder:Order={
    val province: Array[String] = Array[String]("北京市", "天津市", "上海市", "重庆市", "河北省", "山西省", "辽宁省", "吉林省", "黑龙江省", "江苏省", "浙江省", "安徽省", "福建省", "江西省", "山东省", "河南省", "湖北省", "湖南省", "广东省", "海南省", "四川省", "贵州省", "云南省", "陕西省", "甘肃省", "青海省")
    val random = new Random()
    //订单号
    val orderNo: String = UUID.randomUUID.toString
    //用户 userId
    val userId: Int = random.nextInt(10000)
    //商品id
    val goodsId: Int = random.nextInt(1360)
    var goodsMoney: Double = 100 + random.nextDouble * 100
    //商品金额
    goodsMoney = formatDecimal(goodsMoney, 2).toDouble
    var realTotalMoney: Double = 150 + random.nextDouble * 100
    //订单付出金额
    realTotalMoney = formatDecimal(goodsMoney, 2).toDouble

    val payFrom: Int = random.nextInt(5)
    //省份id
    val provinceName: String = province(random.nextInt(province.length))
    val date = new Date
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateStr: String = format.format(date)

    Order(orderNo,userId+"",goodsId+"",goodsMoney+"",realTotalMoney+"",payFrom+"",provinceName)
  }

  /**
   * 生成金额
   * @param d
   * @param newScale
   * @return
   */
  def formatDecimal(d: Double, newScale: Int): String = {
    var pattern = "#."
    var i = 0
    while ( {
      i < newScale
    }) {
      pattern += "#"

      {
        i += 1; i - 1
      }
    }
    val df = new DecimalFormat(pattern)
    df.format(d)
  }




}
