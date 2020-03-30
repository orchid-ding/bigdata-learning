package streaming.input.custom;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.concurrent.atomic.AtomicInteger;

public class MyParalleSource implements ParallelSourceFunction<Integer> {

    private  boolean isCancel = false;

    private static AtomicInteger count = new AtomicInteger(0);

    private int countIndex = 0;

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        while (!isCancel){
            ctx.collect(countIndex);
            countIndex++;
            //每秒生成一条数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isCancel = true;
    }
}
