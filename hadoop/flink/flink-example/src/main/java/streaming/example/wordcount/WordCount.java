//package streaming.example.wordcount;
//
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.windowing.time.Time;
//
///**
// * @author dingchuangshi
// */
//public class WordCount {
//
//    public static void main(String[] args) throws Exception {
//
//        /**
//         * 获取配置参数
//         */
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String hostname = parameterTool.get("hostname");
//        int port = parameterTool.getInt("port");
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStreamSource<String> dataStreamSource = env.socketTextStream(hostname, port);
//
//        SingleOutputStreamOperator<WordCountBean> result = dataStreamSource
//                .flatMap(new WordCountFlatMapFunction()).keyBy("word")
//                .timeWindow(Time.seconds(2), Time.seconds(1))
//                .sum("count");
//
//        result.addSink(new BucketingSink<WordCountBean>("hdfs://node01:8020/kfly/output_1")).setParallelism(1);
//
//        //步骤五：任务启动
//        env.execute("WindowWordCountJava");
//    }
//}
