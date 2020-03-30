package com.kaikeba.flink.table

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.CsvTableSource

/**
 * 基于table & sql api开发 Flink的批处理程序
 */
object ScalaTableSQLBatchWordCount {

  def main(args: Array[String]): Unit = {

      //1. 获取flink的table批处理环境
      val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

      val tEnv: BatchTableEnvironment = BatchTableEnvironment.create(env)

      //2.构建Source数据源
      val tableSource: CsvTableSource = CsvTableSource.builder()
        .path("./data/person.txt")
        .fieldDelimiter(" ")
        .field("id", Types.INT)
        .field("name", Types.STRING)
        .field("age", Types.INT)
        .ignoreParseErrors().lineDelimiter("\r\n")
        .build()

      //将tableSource注册成表 tEnv.registerTableSource("person",tableSource)
      //3. 查询
      //3.1 查询年龄大于30岁的人
      val result1: Table = tEnv.scan("person").filter("age > 30")

      //3.2 统计不同的年龄用户数
      val result2: Table = tEnv.sqlQuery("select age,count(*) from person group by age ")

      //4. 构建sink //打印表的元数据schema信息
      result1.printSchema()

      //保存结果数据到文件中 val tableSink1 = new
      var tableSink1 = new CsvTableSink("./out/result1.txt", "\t", 1, WriteMode.OVERWRITE)
      result1.writeToSink(tableSink1)

      val tableSink2 = new CsvTableSink("./out/result2.txt", "\t", 1, WriteMode.OVERWRITE)
      result2.writeToSink(tableSink2)

      //开启计算
      env.execute()
  }
}