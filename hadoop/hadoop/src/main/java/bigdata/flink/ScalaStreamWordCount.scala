//package bigdata.flink
//
//
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//
///**
//  * 基于scala语言开发Flink的流式处理程序
// */
//object ScalaStreamWordCount {
//
//  def main(args: Array[String]): Unit = {
//
//       //1. 获取flink的流式处理环境
//      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//
//      //2. 构建source数据源
//      val socketTextStream: DataStream[String] = env.socketTextStream("192.168.18.238",9999)
//
//      //3. 数据处理
//      //3.1 切分每一行，获取所有的单词
//      val words: DataStream[String] = socketTextStream.flatMap(_.split(" "))
//
//      //3.2 把每个单词计为1 封装成元组(单词，1)
//      val wordAndOne: DataStream[(String, Int)] = words.map((_,1))
//
//      //3.3 按照单词进行分组
//      val groupByWord: KeyedStream[(String, Int), String] = wordAndOne.keyBy(_._1)
//
//      //3.4 设置时间窗口
//      val timeWindow: WindowedStream[(String, Int), String, TimeWindow] = groupByWord.timeWindow(Time.seconds(5))
//
//      //3.5 相同单词出现的1累加
//      val result: DataStream[(String, Int)] = timeWindow.reduce((v1,v2)=> (v1._1,v1._2+v2._2))
//
//      //4. 构建Sink
//      result.print()
//
//      //5. 启动流式应用程序
//      env.execute("ScalaStreamWordCount")
//  }
//}