package streaming.example.eventtime;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import javax.print.DocFlavor;

/**
 * @author dingchuangshi
 */
public class EventTimeExtractor implements AssignerWithPeriodicWatermarks<Tuple2<String,Integer>> {

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis() - 5000);
    }

    @Override
    public long extractTimestamp(Tuple2<String, Integer> element, long previousElementTimestamp) {
        return previousElementTimestamp;
    }
}
