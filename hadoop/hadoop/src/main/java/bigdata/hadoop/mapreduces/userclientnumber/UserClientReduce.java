package bigdata.hadoop.mapreduces.userclientnumber;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * reduce
 * @author dingchuangshi
 */
public class UserClientReduce extends Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        AtomicInteger sum = new AtomicInteger();

        values.forEach(intWritable -> {
            sum.addAndGet(intWritable.get());
        });

        context.write(key,new IntWritable(sum.get()));
    }
}
