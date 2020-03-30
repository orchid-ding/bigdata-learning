package streaming.windows;


import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.w3c.dom.events.EventException;
import sun.rmi.runtime.Log;

import javax.annotation.Nullable;
import java.awt.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author dingchuangshi
 */
public class WindowsMain {

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        int port = parameterTool.getInt("port");
        String hostname = parameterTool.get("hostname");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies
                .fixedDelayRestart(3, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.enableCheckpointing(1000);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.getCheckpointConfig().setCheckpointTimeout(6000);

        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

        StateBackend stateBackend = new MemoryStateBackend();

        env.setStateBackend(stateBackend);

        DataStreamSource<String> dataStreamSource = env.socketTextStream(hostname, port);

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator =
                dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(",");
                for (String line : split) {
                    out.collect(Tuple2.of(line, 1));
                }
            }
        });

//        SingleOutputStreamOperator<Object> streamOperator = dataStreamSource.flatMap(((value, out) -> {
//            String[] split = value.split(",");
//            for (String line : split) {
//                out.collect(line);
//            }
//        }));

        OutputTag<Tuple2<String,Integer>> output = new OutputTag<Tuple2<String, Integer>>("test-data"){};

        SingleOutputStreamOperator<Tuple2<String, Integer>> result =
                streamOperator
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Integer>>() {

                    Random random = new Random();

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(System.currentTimeMillis() -2000);
                    }

                    @Override
                    public long extractTimestamp(Tuple2<String, Integer> element, long previousElementTimestamp) {
//                        if(random.nextInt(2) >= 1){
//                            return System.currentTimeMillis() - 5000;
//                        }
                        return System.currentTimeMillis();
                    }
                })
                .keyBy(0)
                .window(GlobalWindows.create())
                        .trigger(new Trigger<Tuple2<String, Integer>, GlobalWindow>() {

                            ReducingStateDescriptor<Long> state = new ReducingStateDescriptor<Long>(
                                    "reducing state",
                                    (next,pre)->next + pre,
                                    Types.LONG
                            );

                            @Override
                            public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {

                                ReducingState<Long> count = ctx.getPartitionedState(state);
                                count.add(1L);

                                if(count.get().longValue() == 3){
                                    count.clear();
                                    return TriggerResult.FIRE;
                                }
                                return TriggerResult.CONTINUE;
                            }

                            @Override
                            public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                                return TriggerResult.CONTINUE;
                            }

                            @Override
                            public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                                return TriggerResult.CONTINUE;
                            }

                            @Override
                            public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
                                ctx.getPartitionedState(state).clear();
                            }
                        })
                        .evictor(new Evictor<Tuple2<String, Integer>, GlobalWindow>() {
                            @Override
                            public void evictBefore(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
                                System.out.println("result");
                                Iterator<TimestampedValue<Tuple2<String, Integer>>> iterator = elements.iterator();
                                TimestampedValue<Tuple2<String, Integer>> next = iterator.next();
                                iterator.remove();

                            }

                            @Override
                            public void evictAfter(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
                                System.out.println("开始计算");
                            }
                        }).sum(1);
//                        .allowedLateness(Time.of(3,TimeUnit.SECONDS))
//                .sideOutputLateData(output)
//                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String,Integer>, Tuple, TimeWindow>() {
//                    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
//
//                    @Override
//                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String,Integer>> out) throws Exception {
//                        System.out.println("处理时间：" + dateFormat.format(context.currentProcessingTime()));
//                        System.out.println("window start time : " + dateFormat.format(context.window().getStart()));
//
//                        elements.forEach(value->out.collect(value));
//
//                        System.out.println("window end time  : " + dateFormat.format(context.window().getEnd()));
//                    }
//                });

        result.print();
//
        result
                .getSideOutput(output)
                .map(value -> "迟到数据 -> " + value.f0 + ":" + value.f1)
                .print();

        env.execute("process");

    }

}
