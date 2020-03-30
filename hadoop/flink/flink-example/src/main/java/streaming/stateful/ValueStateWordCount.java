package streaming.stateful;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import streaming.stateful.bean.IndexValue;

/**
 * input key:value 格式 a:1,b:2,c:3
 * 出现相同key三次求平均值
 */
public class ValueStateWordCount {

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");
        // 默认几次计算一次
        int number = parameterTool.getInt("number", 3);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> dataStream = env.socketTextStream(hostname, port);

        SingleOutputStreamOperator<IndexValue> singleOutputStreamOperator = dataStream.flatMap(new FlatMapFunction<String, IndexValue>() {
            @Override
            public void flatMap(String value, Collector<IndexValue> out) throws Exception {
                String[] splits = value.split(",");
                for (String line : splits) {
                    String[] indexValue = line.split(":");
                    if (indexValue.length == 2) {
                        out.collect(IndexValue.of(indexValue[0], Long.valueOf(indexValue[1])));
                    }
                }
            }
        });
        singleOutputStreamOperator
                .keyBy("index")
//                .flatMap(new CountWindowsAvgWithValueState(number))
//                .flatMap(new CountWindowsAvgWithListState(number))
//                .flatMap(new CountWindowsAvgWithMapState(number))
                .print();

        env.execute(ValueStateWordCount.class.getSimpleName());
    }
}