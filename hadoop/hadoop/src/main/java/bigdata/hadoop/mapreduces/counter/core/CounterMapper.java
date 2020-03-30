package bigdata.hadoop.mapreduces.counter.core;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 *  自定义计数器
 * @author dingchuangshi
 */
public class CounterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 切分搜狗输入法，日志的各个字段
        String[] logs = value.toString().split("\t");
        System.out.println(logs.length);
        // 不需要reduce进行统计，所以reduce输入value结果为空
        NullWritable nullWritable = NullWritable.get();
        // 获取计数器
        Counter counter = context.getCounter("DataClean","sougouClean");

        // 正确的格式为6个字段，如果切分格式不符合则计数
        if(logs.length != 6){
            counter.increment(1);
        }else{
            context.write(value,nullWritable);
        }

    }
}
