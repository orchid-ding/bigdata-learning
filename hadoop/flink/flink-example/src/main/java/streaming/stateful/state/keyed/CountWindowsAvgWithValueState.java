package streaming.stateful.state.keyed;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.operators.translation.TupleWrappingCollector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import streaming.stateful.bean.IndexValue;

/**
 * 1. 每三个值求一次平均值
 * @author dingchuangshi
 */
public class CountWindowsAvgWithValueState extends RichFlatMapFunction<IndexValue,IndexValue> {

    private int number;

    private ValueState<Tuple2<Long,Long>> countValueState;

    public CountWindowsAvgWithValueState(int number) {
        this.number = number;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Tuple2<Long,Long>> countValueStateDescriptor = new ValueStateDescriptor<>(
                "countValueState", Types.TUPLE(Types.LONG,Types.LONG));
        countValueState = getRuntimeContext().getState(countValueStateDescriptor);
    }

    @Override
    public void flatMap(IndexValue elementState, Collector<IndexValue> out) throws Exception {

        Tuple2<Long,Long> currentState = countValueState.value();
        if(currentState == null){
            currentState = Tuple2.of(0L,0L);
        }
        currentState.f0 += 1;
        currentState.f1 += elementState.getValue();

        countValueState.update(currentState);

        if(currentState.f0 > 3){
            out.collect(IndexValue.of(elementState.getIndex(),Long.valueOf(currentState.f1 / currentState.f0)));
            countValueState.clear();
        }

    }
}
