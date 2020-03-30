package streaming.example.order.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author dingchuangshi
 */
public class OrderMegerMain {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> orderInfoTextFile = env.readTextFile( "./data/order.orderinfo.txt");


        DataSet<OrderInfo> orderInfoMapOperator = orderInfoTextFile.map(new MapFunction<String, OrderInfo>() {
            @Override
            public OrderInfo map(String value) throws Exception {
                String[] splits = value.split(",");
                return new OrderInfo(splits[0], Double.valueOf(splits[2]), splits[1]);
            }
        });

        DataSet<String> orderDetailTextFile = env.readTextFile("./data/order.orderidetail.txt");

        DataSet<OrderDetailInfo> detailInfoMapOperator = orderDetailTextFile.map(line->{
            String[] splits = line.split(",");
            return new OrderDetailInfo(splits[0], splits[1], splits[2]);
        });

        DataSet<Tuple2<OrderInfo,OrderDetailInfo>> result = orderInfoMapOperator.join(detailInfoMapOperator)
                .where("id")
                .equalTo("id");


        result.writeAsFormattedText("./data/formattext",orderInfo->{
            OrderInfo info = orderInfo.f0;
            OrderDetailInfo detail = orderInfo.f1;

            String results = info.getId() + "," + info.getName() + "," + info.getPrice();
            if(detail != null){
                String detailStr = "," + detail.getOrderTime() + "," + detail.getOrderProvince();
                results += detailStr;
            }
            return  results;
        }).setParallelism(1);

        env.execute("Order Merge");

    }

}
