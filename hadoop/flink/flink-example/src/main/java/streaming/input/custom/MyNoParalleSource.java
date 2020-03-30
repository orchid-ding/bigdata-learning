package streaming.input.custom;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author dingchuangshi
 */
public class MyNoParalleSource implements SourceFunction<Integer> {

    private static volatile boolean isCancel = false;

    private static AtomicInteger count = new AtomicInteger(0);
    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        while (!isCancel){
            ctx.collect(count.incrementAndGet());
            //每秒生成一条数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isCancel = true;
    }
}
