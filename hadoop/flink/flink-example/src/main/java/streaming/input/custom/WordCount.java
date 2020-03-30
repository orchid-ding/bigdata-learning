package streaming.input.custom;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dingchuangshi
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<Integer> dataStreamSource2 = env.addSource(new MyNoParalleSource()).setParallelism(1);
        DataStreamSource<Integer> dataStreamSource = env.addSource(new MyParalleSource()).setParallelism(1);
/// 1、 union
//        DataStream<Integer> union = dataStreamSource.union(dataStreamSource2);
//
//        union.map(new MapFunction<Integer, Long>() {
//            @Override
//            public Long map(Integer integer) throws Exception {
//                System.out.println(Thread.currentThread().getId() +"->" +integer);
//                return integer.longValue();
//            }
//        }).setParallelism(2);

/// 2. connect
//        SingleOutputStreamOperator<String> map = dataStreamSource2.map(new MapFunction<Integer, String>() {
//            @Override
//            public String map(Integer integer) throws Exception {
//                return "str_" + integer;
//            }
//        });
//        ConnectedStreams<Integer, String> connect = dataStreamSource.connect(map);
//        connect.map(new CoMapFunction<Integer, String, Object>() {
//            @Override
//            public Object map1(Integer value) throws Exception {
//                return null;
//            }
//
//            @Override
//            public Object map2(String value) throws Exception {
//                return null;
//            }
//        });
//
//        connect.flatMap(new CoFlatMapFunction<Integer, String, Object>() {
//            @Override
//            public void flatMap1(Integer value, Collector<Object> out) throws Exception {
//                System.out.println("1"+value);
//            }
//
//            @Override
//            public void flatMap2(String value, Collector<Object> out) throws Exception {
//                System.out.println("2" +value);
//            }
//        });

///      3. split
        SplitStream<Integer> split = dataStreamSource.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                List<String> outPut = new ArrayList<>();
                String flag = value % 2 == 0 ? "even" : "odd";
                outPut.add(flag);
                return outPut;
            }
        });
//
//        DataStream<Integer> even = split.select("even");
//        System.out.println("even");
//        even.print();

//        DataStream<Integer> odd = split.select("odd");
//        System.out.println("odd");
//        odd.print();
//
        DataStream<Integer> select = split.select("even", "odd");
        System.out.println("all");
        select.writeAsText("./data/word.txt");

        env.execute("executor custom source");

    }
}
