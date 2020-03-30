package streaming.stateful;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import streaming.stateful.bean.IndexValue;
import streaming.stateful.state.keyed.CountWordAggregationState;

/**
 * @author dingchuangshi
 */
public class ReduceStateValueSum {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> dataStreamSource = env.socketTextStream(hostname, port);

        SingleOutputStreamOperator<IndexValue> streamOperator = dataStreamSource.flatMap(new FlatMapFunction<String, IndexValue>() {
            @Override
            public void flatMap(String value, Collector<IndexValue> out) throws Exception {
                String[] splits = value.split(",");
                try{
                    out.collect(IndexValue.of(splits[0],Long.valueOf(splits[1])));
                }catch (Exception e){
                    System.err.println(e.getMessage());
                }
            }
        });

        streamOperator
                .keyBy("index")
//                .flatMap(new CountWindowsSumReduceState())
                .flatMap(new CountWordAggregationState())
                .print();

        env.execute("custom sum add");


    }
}
