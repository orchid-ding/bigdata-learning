package streaming.stateful.state.operator;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import streaming.stateful.bean.IndexValue;

import java.util.ArrayList;
import java.util.List;

public class CustomSinkListState implements SinkFunction<IndexValue> , CheckpointedFunction {


    /**
     *  用于缓存结果数据的
     */
    private List<IndexValue> listData;

    /**
     * 用于保存内存中的状态信息
     */
    private ListState<IndexValue> checkpointListState;

    /**
     * 表示内存中数据的大小阈值
     */
    private int threshold;
    /**
     * 每次进入一条数据运行一次
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(IndexValue value, Context context) throws Exception {
        listData.add(value);
        if(listData.size() >= threshold){
            System.out.println("自定义格式 -> " + listData);
            listData.clear();
        }
    }


    public CustomSinkListState(int threshold) {
        this.threshold = threshold;
        this.listData = new ArrayList<>();
    }

    /**
     * 用于将内存中数据保存到状态中
     * @param context
     * @throws Exception
     */

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointListState.clear();
        for (int i = 0; i < listData.size(); i++) {
            checkpointListState.add(listData.get(i));
        }
    }

    /**
     * 用于在程序挥发的时候从状态中恢复数据到内存
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<IndexValue> listStateDescriptor = new ListStateDescriptor<>("nameState", IndexValue.class);

        checkpointListState = context.getOperatorStateStore().getListState(listStateDescriptor);

        if(context.isRestored()){
            checkpointListState.get().forEach(value->{
                listData.add(value);
            });
        }
    }
}
