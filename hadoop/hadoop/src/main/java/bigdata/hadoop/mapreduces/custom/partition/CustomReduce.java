package bigdata.hadoop.mapreduces.custom.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author dingchuangshi
 */
public class CustomReduce extends Reducer <Text, IntWritable,Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        AtomicInteger integer = new AtomicInteger(0);
        values.forEach(intWritable -> {
            integer.addAndGet(intWritable.get());
        });
        context.write(key,new IntWritable(integer.get()));
    }
}
