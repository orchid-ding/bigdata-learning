package streaming.stateful.state.keyed;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import streaming.stateful.bean.IndexValue;

/**
 * @author dingchuangshi
 */
public class CountWordAggregationState extends RichFlatMapFunction<IndexValue,IndexValue> {

    private AggregatingState<IndexValue,String> aggregatingState;

    private ReducingState<Long> reducingState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        AggregatingStateDescriptor<IndexValue, String, String> aggregatingStateDescriptor = new AggregatingStateDescriptor<>(
                "aggregatingState",
                new AggregateFunction<IndexValue, String, String>() {
                    @Override
                    public String createAccumulator() {
                        return " : indexValue -> ";
                    }

                    @Override
                    public String add(IndexValue value, String accumulator) {
                        if (" : indexValue -> ".equals(accumulator)) {
                            return value.getIndex() + "_" + accumulator + value.getValue();
                        }
                        return accumulator + " and " + value.getValue();
                    }

                    @Override
                    public String getResult(String accumulator) {
                        return accumulator;
                    }

                    @Override
                    public String merge(String a, String b) {
                        return a + b;
                    }
                },
                Types.STRING
        );

        ReducingStateDescriptor<Long> reducingStateDescriptor = new ReducingStateDescriptor<>(
                "reducingState",
                new ReduceFunction<Long>() {
                    @Override
                    public Long reduce(Long value1, Long value2) throws Exception {
                        return value1 + value2;
                    }
                },
                Types.LONG
        );

        aggregatingState = getRuntimeContext().getAggregatingState(aggregatingStateDescriptor);
        reducingState = getRuntimeContext().getReducingState(reducingStateDescriptor);
    }

    @Override
    public void flatMap(IndexValue value, Collector<IndexValue> out) throws Exception {
        aggregatingState.add(value);
        reducingState.add(value.getValue());
        out.collect(IndexValue.of(aggregatingState.get(),reducingState.get()));
    }
}
