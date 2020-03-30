package top.kfly.run;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import top.kfly.config.CustomConfig;
import top.kfly.core.CustomFlatCoMapFunction;
import top.kfly.source.CustomRedisSource;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author dingchuangshi
 */
public class DataCleanMain {

    public static void main(String[] args) throws Exception {
        CustomConfig customConfig = new CustomConfig(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointTimeout(6000);

        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers",customConfig.getBootstrapServers());
        consumerProperties.put("group.id",customConfig.getGroupId());
        consumerProperties.put("enable.auto.commit",customConfig.getAutoCommit());
        consumerProperties.put("auto.offset.reset",customConfig.getOffsetReset());

        FlinkKafkaConsumer010<String> flinkKafkaConsumer010 = new FlinkKafkaConsumer010<>(customConfig.getTopics(), new SimpleStringSchema(), consumerProperties);
        DataStreamSource<String> dataStreamSource = env.addSource(flinkKafkaConsumer010);

        CustomRedisSource redisSource = new CustomRedisSource(customConfig);
        DataStream<Map<String, String>> redisBroadcast = env.addSource(redisSource).broadcast();

        SingleOutputStreamOperator<String> etlResultData = dataStreamSource
                .connect(redisBroadcast)
                .flatMap(new CustomFlatCoMapFunction());

        etlResultData
                .writeAsText(customConfig.getOutPutPath())
                .setParallelism(1);

        env.execute("data-clean");
    }
}
