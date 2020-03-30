package batch;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @author dingchuangshi
 */
public class WordCount{

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSet = env.fromElements("Who's there?",
                "I think I hear them. Stand, ho! Who's there?");

        FlatMapOperator<String, Tuple2<String, Integer>> flatMap = dataSet.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String lines, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] splits = lines.split(" ");
                for(String word:splits){
                    out.collect(new Tuple2(word, 1));
                }
            }
        });

        dataSet.map(new MapFunction<String, Object>() {
            @Override
            public Object map(String s) throws Exception {
                return s;
            }
        });

        dataSet.mapPartition(new MapPartitionFunction<String, Object>() {
            @Override
            public void mapPartition(Iterable<String> iterable, Collector<Object> collector) throws Exception {
                iterable.forEach(str->{
                    collector.collect(str);
                });
            }
        });


        IntCounter intCounter = new IntCounter();

        MapOperator<String, Object> stringObjectMapOperator = dataSet.map(new RichMapFunction<String, Object>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("line-number", intCounter);

                List<Object> name = getRuntimeContext().getBroadcastVariable("name");
            }

            @Override
            public Object map(String s) throws Exception {
                intCounter.add(1);
                return null;
            }
        }).withBroadcastSet(null, "brocast");


        AggregateOperator<Tuple2<String, Integer>> sum = flatMap.groupBy(0).sum(1);


        JobExecutionResult execute = env.execute();
        Object accumulatorResult = execute.getAccumulatorResult("line-number");

    }
}
