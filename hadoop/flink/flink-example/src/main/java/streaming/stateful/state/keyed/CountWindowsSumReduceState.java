package streaming.stateful.state.keyed;

import akka.stream.impl.ReducerState;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import streaming.stateful.bean.IndexValue;

/**
 * @author dingchuangshi
 */
public class CountWindowsSumReduceState extends RichFlatMapFunction<IndexValue,IndexValue> {

    private ReducingState<Long> reducingState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ReducingStateDescriptor<Long> stateDescriptor = new ReducingStateDescriptor<Long>(
                "reduce sum",
                new ReduceFunction<Long>() {
                    @Override
                    public Long reduce(Long pre, Long cur) throws Exception {
                        return pre + cur;
                    }
                },
                Types.LONG);
        reducingState = getRuntimeContext().getReducingState(stateDescriptor);
    }

    @Override
    public void flatMap(IndexValue value, Collector<IndexValue> out) throws Exception {
        reducingState.add(value.getValue());
        out.collect(IndexValue.of(value.getIndex(),reducingState.get()));
    }
}
