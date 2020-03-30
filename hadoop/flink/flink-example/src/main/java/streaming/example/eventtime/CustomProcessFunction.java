package streaming.example.eventtime;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author dingchuangshi
 */
public class CustomProcessFunction extends ProcessWindowFunction<Tuple2<String,Integer>,Tuple2<String,Integer>, Tuple, TimeWindow> {

    FastDateFormat dataFormat = FastDateFormat.getInstance("HH:mm:ss");

    /**
     * 当一个window触发计算的时候会调用这个方法
     * @param tuple2 key
     * @param context operator的上下文
     * @param elements 指定window的所有元素
     * @param out 用户输出
     */
    @Override
    public void process(Tuple tuple2, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
//        System.out.println("当天系统的时间："+dataFormat.format(System.currentTimeMillis()));
//        System.out.println("Window的处理时间："+dataFormat.format(context.currentProcessingTime()));
//        System.out.println("Window的开始时间："+dataFormat.format(context.window().getStart()));
//        System.out.println("Window的结束时间："+dataFormat.format(context.window().getEnd()));

        int sum = 0;
        for (Tuple2<String, Integer> ele : elements) {
            sum += 1;
        }
        // 输出单词出现的次数
        out.collect(Tuple2.of(tuple2.getField(0), sum));


    }
}
