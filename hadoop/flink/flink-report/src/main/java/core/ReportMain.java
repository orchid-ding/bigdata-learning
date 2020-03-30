package core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import config.CustomConfig;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author dingchuangshi
 */
public class ReportMain {

    private static Logger logger = LoggerFactory.getLogger(ReportMain.class);

    public static void main(String[] args) throws Exception {

        CustomConfig customConfig = new CustomConfig(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setRestartStrategy(RestartStrategies
                .fixedDelayRestart(3, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));

        env.enableCheckpointing(1000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties properties = customConfig.getConsumerProperites();

        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<>(
                customConfig.getTopics(),
                new SimpleStringSchema(),
                properties);

        DataStreamSource<String> dataStreamSource = env.addSource(kafkaConsumer);

        SingleOutputStreamOperator<Tuple3<Long, String, String>> preData = dataStreamSource.map(new MapFunction<String, Tuple3<Long, String, String>>() {
            /**
             * Long:time
             * String: type
             * String: area
             * @return
             * @throws Exception
             */
            @Override
            public Tuple3<Long, String, String> map(String line) throws Exception {
                JSONObject jsonObject = JSON.parseObject(line);
                String dt = jsonObject.getString("dt");
                String type = jsonObject.getString("type");
                String area = jsonObject.getString("area");
                long time = 0;

                try {
                    FastDateFormat sdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
                    time = sdf.parse(dt).getTime();
                } catch (Exception e){
                    logger.error("时间解析失败，dt:" + dt, e.getCause());
                }


                return Tuple3.of(time, type, area);
            }
        });

        /**
         * 过滤数据
         */
        SingleOutputStreamOperator<Tuple3<Long, String, String>> outputStreamOperator = preData.filter(filter -> filter.f0 != 0);

        /**
         * 收集迟到太久的数据
         */
        OutputTag<Tuple3<Long,String,String>> outputTag=
                new OutputTag<Tuple3<Long,String,String>>("late-date"){};


        /**
         * 进行窗口的统计操作
         * 统计的过去的一分钟的每个大区的,不同类型的有效视频数量
         */

        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> resultData =
                outputStreamOperator
                        .assignTimestampsAndWatermarks(new MyWaterMark())
                .keyBy(1, 2)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .sideOutputLateData(outputTag)
//                        .process(new ProcessWindowFunction<Tuple3<Long, String, String>, Object, Tuple, TimeWindow>() {
//                            @Override
//                            public void process(Tuple tuple, Context context, Iterable<Tuple3<Long, String, String>> elements, Collector<Object> out) throws Exception {
//                             FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
//                                System.out.println("====================================================");
//                                System.out.println(dateFormat.format(System.currentTimeMillis()));
//                                System.out.println(dateFormat.format(context.currentProcessingTime()));
//                                System.out.println(dateFormat.format(context.currentWatermark()));
//                                System.out.println(dateFormat.format(context.window().getStart()));
//                                System.out.println(dateFormat.format(context.window().getEnd()));
//                            }
//                        });
                .apply(new MySumFuction());

        /**
         * 收集到延迟太多的数据，业务里面要求写到Kafka
         */
        SingleOutputStreamOperator<String> sideOutput =
                resultData.getSideOutput(outputTag).map(line -> line.toString());
        sideOutput
                .writeAsText(customConfig.getOutPutErrPath())
                .setParallelism(1);
        /**
         * 业务里面需要吧数据写到ES里面
         * 而我们公司是需要把数据写到kafka
         */

        resultData
                .writeAsText(customConfig.getOutPutPath())
                .setParallelism(1);


        env.execute("DataReport");

    }
}
