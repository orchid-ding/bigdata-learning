//package bigdata.flink
//
//import org.apache.flink.core.fs.FileSystem.WriteMode
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.table.api.Table
//import org.apache.flink.table.api.scala.StreamTableEnvironment
//import org.apache.flink.table.sinks.CsvTableSink
//import org.apache.flink.api.scala._
//
///**
// * 基于table & sql api开发 Flink的流式处理程序
// */
//object ScalaTableSQLStreamWordCount {
//
//  case class User(id:Int,name:String,age:Int)
//
//  def main(args: Array[String]): Unit = {
//
//    //1. 获取flink的table流式处理的环境
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//
//    val streamSQLEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
//    //2. 构建Source数据源
//    /**
//      * 101,zhangsan,18
//      * 102,lisi,20
//      * 103,wangwu,25
//      * 104,zhaoliu,15
//      */
//    val socketDataStream: DataStream[String] = env.socketTextStream("node01",9999)
//
//    val userDataStream: DataStream[User] = socketDataStream.map(_.split(",")).map(y=>User(y(0).toInt,y(1),y(2).toInt ))
//
//    //3. 将流注册成一张表
//    streamSQLEnv.registerDataStream("userTable",userDataStream)
//
//    //4. 使用table && sql api来查询数据
//
//     // 使用table 的api查询年龄大于20岁的人
//    val result1: Table = streamSQLEnv.scan("userTable").filter("age >20")
//
//    //使用sql 的api查询
//    val result2: Table = streamSQLEnv.sqlQuery(" select * from userTable ")
//
//    //5. 构建Sink
//    val tableSink1 = new CsvTableSink("./out/tableSink1.txt","\t",1,WriteMode.OVERWRITE)
//    result1.writeToSink(tableSink1)
//
//    val tableSink2 = new CsvTableSink("./out/tableSink2.txt","\t",1,WriteMode.OVERWRITE)
//    result2.writeToSink(tableSink2) //开启执行流式计算
//
//    env.execute() }
//}