//package bigdata.flink
//
//import org.apache.flink.api.scala._
//
///**
// * 通过Scala语言开发Flink 批处理程序
// */
//object ScalaWorkCount {
//
//  def main(args: Array[String]): Unit = {
//
//    // 获取Flink批处理执行环境
//    var env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment;
//
//    // 初始化原数据
//    var data:DataSet[String] = env.fromCollection(List("bigdata.hadoop mapreduce","bigdata.hadoop spark","spark core"))
//
//    // 数据处理，切分每一行数据获取所有单词
//    var words : DataSet[String] = data.flatMap(_.split(" "))
//
//    // 把每个单词记为1，封装成元组
//    var wordAndOne:DataSet[(String,Int)] = words.map((_,1))
//
//    // 按照单词进行分组
//    var groupByWord:GroupedDataSet[(String,Int)] = wordAndOne.groupBy(0)
//
//    // 对相同的单词进行分组
//    var aggregateDataSet: AggregateDataSet[(String,Int)] = groupByWord.sum(1)
//
//    // 打印
//    aggregateDataSet.print()
//  }
//}
