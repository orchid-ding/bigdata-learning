package core;


import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 *
 * @author dingchuangshi
 */
public class MyWaterMark
        implements AssignerWithPeriodicWatermarks<Tuple3<Long,String,String>> {

    long currentMaxTimestamp=0L;

    /**
     *  允许乱序时间。
     */
    final long maxOutputOfOrderness=20000L;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        FastDateFormat sdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        System.out.println("waterm" + sdf.format(currentMaxTimestamp));
        System.out.println(maxOutputOfOrderness);
        System.out.println("waterm"+sdf.format(currentMaxTimestamp - maxOutputOfOrderness));
        return new Watermark(currentMaxTimestamp - maxOutputOfOrderness);
    }

    @Override
    public long extractTimestamp(Tuple3<Long, String, String>
                                             element, long l) {
        Long timeStamp = element.f0;
        currentMaxTimestamp=Math.max(timeStamp,currentMaxTimestamp);
        return timeStamp;
    }
}
