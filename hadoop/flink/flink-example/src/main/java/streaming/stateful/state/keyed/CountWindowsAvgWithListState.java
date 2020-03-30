package streaming.stateful.state.keyed;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;
import streaming.stateful.bean.IndexValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 1. 每三个值求一次平均值
 * @author dingchuangshi
 */
public class CountWindowsAvgWithListState extends RichFlatMapFunction<IndexValue,IndexValue> {

    private int number;

    private ListState<IndexValue> countListState;

    public CountWindowsAvgWithListState(int number) {
        this.number = number;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<IndexValue> countListStateDescriptor = new ListStateDescriptor<>(
                "countListState", Types.POJO(IndexValue.class));
        countListState = getRuntimeContext().getListState(countListStateDescriptor);
    }

    @Override
    public void flatMap(IndexValue elementState, Collector<IndexValue> out) throws Exception {

        Iterable<IndexValue> currentState = countListState.get();
        if(currentState == null){
            countListState.addAll(Collections.EMPTY_LIST);
        }
        countListState.add(elementState);


        ArrayList<IndexValue> indexValues = Lists.newArrayList(countListState.get());
        if(indexValues.size() >= number){
            int count = 0;
            for (int i = 0; i < indexValues.size(); i++) {
                count += indexValues.get(i).getValue();
            }
            out.collect(IndexValue.of(elementState.getIndex(),Long.valueOf(count / indexValues.size())));
            countListState.clear();
        }
    }
}
