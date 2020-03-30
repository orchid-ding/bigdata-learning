package streaming.example.order.connect;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import streaming.example.order.join.OrderDetailInfo;
import streaming.example.order.join.OrderInfo;
import java.util.concurrent.TimeUnit;

/**
 * @author dingchuangshi
 */
public class OrderMergeMain {


    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String inputOrderDir = parameterTool.get("inputOrderDir");
        String inputDetailDir = parameterTool.get("inputDetailDir");

        String hostname = parameterTool.get("hostname");
        int orderPort = parameterTool.getInt("orderPort");
        int detailPort = parameterTool.getInt("detailPort");

        /**
         * 固定时间间隔重启
         */
        env.setRestartStrategy(RestartStrategies
                .fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

        StateBackend stateBackend = new FsStateBackend("hdfs://node01:8020/kfly/checkpoint");
        env.setStateBackend(stateBackend);

        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(1000);
        // 高级选项：
        // 设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(6000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<String> orderInfoData = env.socketTextStream(hostname,orderPort);
///                env.addSource(new FileCustomSource(inputOrderDir));
        DataStreamSource<String> orderDetailData = env.socketTextStream(hostname,detailPort);
///        env.addSource(new FileCustomSource(inputDetailDir));

        SingleOutputStreamOperator<OrderInfo> orderInfo = orderInfoData.map(line -> {
            String[] splits = line.split(",");
            return new OrderInfo(splits[0], Double.valueOf(splits[2]), splits[1]);
        });

        SingleOutputStreamOperator<OrderDetailInfo> orderDetail = orderDetailData.map(line -> {
            String[] splits = line.split(",");
            return new OrderDetailInfo(splits[0], splits[1], splits[2]);
        });

//        orderInfo.connect(orderDetail)
//                .keyBy("id","id")
//                .flatMap(new ValueFlatMapState())
//                .addSink(new BucketingSink<String>("hdfs://node01:8020/kfly/output")).setParallelism(1);

        env.execute("data merge");

    }
}
