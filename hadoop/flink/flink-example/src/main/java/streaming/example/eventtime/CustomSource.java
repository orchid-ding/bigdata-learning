package streaming.example.eventtime;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author dingchuangshi
 */
public class CustomSource implements SourceFunction<String> {

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

//        ctx.collectWithTimestamp();
    }

    @Override
    public void cancel() {

    }
}
