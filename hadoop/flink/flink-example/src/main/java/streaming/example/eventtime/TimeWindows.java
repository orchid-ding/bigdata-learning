package streaming.example.eventtime;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * @author dingchuangshi
 */
public class TimeWindows {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");

        env.setRestartStrategy(RestartStrategies
                .fixedDelayRestart(3, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000);

        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        env.getCheckpointConfig().setCheckpointTimeout(6000);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<String> dataStreamSource = env.addSource(new TestSouce());
///                env.addSource(new CustomSource());

        SingleOutputStreamOperator<Tuple2<String,Integer>> operator = dataStreamSource
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] splits = value.split(",");
                        for (String line : splits) {
                            out.collect(Tuple2.of(line,1));
                        }
                    }
                });

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = operator
                .assignTimestampsAndWatermarks(new EventTimeExtractor())
                .keyBy(0)
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .process(new CustomProcessFunction());
///                .sum(1).setParallelism(1);

        result.print();

        env.execute("sum");
    }

    public static class TestSouce implements SourceFunction<String> {
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            // 控制大约在 10 秒的倍数的时间点发送事件
            String currTime = String.valueOf(System.currentTimeMillis());
            while (Integer.valueOf(currTime.substring(currTime.length() - 4)) > 100) {
                currTime = String.valueOf(System.currentTimeMillis());
                continue;
            }
            System.out.println("开始发送事件的时间：" + dateFormat.format(System.currentTimeMillis()));
            // 第 13 秒发送两个事件
            TimeUnit.SECONDS.sleep(13);
            ctx.collectWithTimestamp("hadoop", System.currentTimeMillis());
            // 产生了一个事件，但是由于网络原因，事件没有发送
            String event = "hadoop";
            Long eventTime = System.currentTimeMillis();
            // 第 16 秒发送一个事件
            TimeUnit.SECONDS.sleep(3);
            ctx.collectWithTimestamp("hadoop" ,System.currentTimeMillis());
            // 第 19 秒的时候发送
            TimeUnit.SECONDS.sleep(3);
            ctx.collectWithTimestamp(event,eventTime);

            TimeUnit.SECONDS.sleep(300);

        }

        @Override
        public void cancel() {

        }
    }
}
