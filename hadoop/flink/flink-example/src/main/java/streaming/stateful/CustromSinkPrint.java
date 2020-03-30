package streaming.stateful;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import streaming.stateful.bean.IndexValue;
import streaming.stateful.state.operator.CustomSinkListState;

/**
 * @author dingchuangshi
 */
public class CustromSinkPrint {

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");

        int number = parameterTool.getInt("number");

        Configuration conf = new Configuration();
        conf.setString("state.checkpoints.dir","file:///Users/dingchuangshi/Projects/java/hadoop/flink/data");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        // start a checkpoint every 1000 ms
        env.enableCheckpointing(10);

//        // advanced options:
//
//        // set mode to exactly-once (this is the default)
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//
//        // make sure 500 ms of progress happen between checkpoints
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
//
//        // checkpoints have to complete within one minute, or are discarded
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//
//        // allow only one checkpoint to be in progress at the same time
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//
//        // enable externalized checkpoints which are retained after job cancellation
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//
//        // allow job recovery fallback to checkpoint when there is a more recent savepoint
//        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        DataStreamSource<String> dataStreamSource = env.socketTextStream(hostname, port);

        SingleOutputStreamOperator<IndexValue> streamOperator = dataStreamSource.flatMap(new FlatMapFunction<String, IndexValue>() {
            @Override
            public void flatMap(String value, Collector<IndexValue> out) throws Exception {
                String[] splits = value.split(",");
//                try {
                    out.collect(IndexValue.of(splits[0], Long.valueOf(splits[1])));
//                } catch (Exception e) {
//                    e.printStackTrace();
//                    System.err.println(value);
//                }
            }
        });

        CustomSinkListState customSinkListState = new CustomSinkListState(number);
        streamOperator.addSink(customSinkListState).setParallelism(1);

        env.execute("custom sink list state");
    }
}
