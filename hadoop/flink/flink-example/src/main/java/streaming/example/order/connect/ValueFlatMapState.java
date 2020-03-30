package streaming.example.order.connect;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import streaming.example.order.join.OrderDetailInfo;
import streaming.example.order.join.OrderInfo;

public class ValueFlatMapState extends RichCoFlatMapFunction<OrderInfo, OrderDetailInfo,String> {

    private ValueState<OrderInfo> orderInfoValueState;

    private ValueState<OrderDetailInfo> orderDetailInfoValueState;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ValueStateDescriptor<OrderInfo> orderInfoValueStateDescriptor = new ValueStateDescriptor<>(
                "orderInfoValueState",
                Types.POJO(OrderInfo.class)
        );
        orderInfoValueState = getRuntimeContext().getState(orderInfoValueStateDescriptor);

        ValueStateDescriptor<OrderDetailInfo> orderDetailValueStateDescriptor = new ValueStateDescriptor<>(
                "orderDetailValueState",
                Types.POJO(OrderDetailInfo.class)
        );
        orderDetailInfoValueState = getRuntimeContext().getState(orderDetailValueStateDescriptor);



    }

    @Override
    public void flatMap1(OrderInfo value, Collector<String> out) throws Exception {
        OrderDetailInfo orderDetailInfo = orderDetailInfoValueState.value();
        if(orderDetailInfo != null){
            orderDetailInfoValueState.clear();
            out.collect(get(value,orderDetailInfo));
        }else {
            orderInfoValueState.update(value);
        }
    }

    @Override
    public void flatMap2(OrderDetailInfo value, Collector<String> out) throws Exception {
        OrderInfo orderInfo = orderInfoValueState.value();
        if(orderInfo != null){
            orderInfoValueState.clear();
            out.collect(get(orderInfo,value));
        }else {
            orderDetailInfoValueState.update(value);
        }
    }

    private String get(OrderInfo info,OrderDetailInfo detail){
        String results = info.getId() + "," + info.getName() + "," + info.getPrice();
        if(detail != null){
            String detailStr = "," + detail.getOrderTime() + "," + detail.getOrderProvince();
            results += detailStr;
        }
        return  results;
    }
}
