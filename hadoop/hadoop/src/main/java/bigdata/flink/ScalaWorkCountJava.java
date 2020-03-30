package bigdata.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author dingchuangshi
 */
public class ScalaWorkCountJava {
    public static void main(String[] args) throws Exception {

        // 获取Flink批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

        // 初始化原数据
        DataSource<String> data = env.fromElements("hadoop mapreduce", "hadoop spark", "spark core");

        // 数据处理
        DataSet<Tuple2<String, Integer>> words = data
                .flatMap((FlatMapFunction<String, Tuple2<String,Integer>>)(s,out)->{
                    for (String word: s.split(" ")) {
                        out.collect(new Tuple2<String,Integer>(word,1));
                    }
                })
                .groupBy(0)
                .sum(1);

        // sink
        words.print();
    }
}
