package streaming.stateful;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 单词计数
 * 没有设置并行度
 * 默认电脑有几个cpu core就会有几个并行度
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888);


        MapStateDescriptor<String, String> mapStateDescriptor =
                new MapStateDescriptor<>("brock", Types.STRING(), Types.STRING());

        BroadcastStream<String> broadcast = dataStream.broadcast(mapStateDescriptor);


        SingleOutputStreamOperator<Tuple2<String, Long>> wordOneStream = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordOneStream
                .keyBy(0)
                .sum(1);

        result.print();

        env.execute(WordCount.class.getSimpleName());
    }
}