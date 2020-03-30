package streaming.stateful.state.keyed;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.util.Collector;
import streaming.stateful.bean.IndexValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

/**
 * 1. 每三个值求一次平均值
 * @author dingchuangshi
 */
public class CountWindowsAvgWithMapState extends RichFlatMapFunction<IndexValue,IndexValue> {

    private int number;

    private MapState<String,IndexValue> countMapState;

    public CountWindowsAvgWithMapState(int number) {
        this.number = number;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<String,IndexValue> countListStateDescriptor = new MapStateDescriptor<String, IndexValue>(
                "countMapState",
                Types.STRING,
                Types.POJO(IndexValue.class)
        );
        countMapState = getRuntimeContext().getMapState(countListStateDescriptor);
    }

    @Override
    public void flatMap(IndexValue elementState, Collector<IndexValue> out) throws Exception {

        Iterable<IndexValue> valueIterable = countMapState.values();
        if(valueIterable == null){
            countMapState.putAll(Collections.EMPTY_MAP);
        }
        countMapState.put(UUID.randomUUID().toString(),elementState);

        ArrayList<IndexValue> indexValues = Lists.newArrayList(countMapState.values());
        if(indexValues.size() >= number){
            int count = 0;
            for (int i = 0; i < indexValues.size(); i++) {
                count += indexValues.get(i).getValue();
            }
            out.collect(IndexValue.of("map state -> " + elementState.getIndex(),Long.valueOf(count / indexValues.size())));
            countMapState.clear();
        }
    }
}
