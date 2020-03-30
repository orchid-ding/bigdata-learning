package bigdata.hadoop.mapreduces.custom.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * @author dingchuangshi
 */
public class CustomMapper  extends Mapper<LongWritable, Text,Text, IntWritable> {

    Text value = new Text();

    IntWritable intWritable = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 字符串转为字节数组
        String[] mapperData = value.toString().split(" ");
        for (int i = 0; i < mapperData.length; i++) {
            value.set(mapperData[i]);
            if(!mapperData[i].isEmpty()) {
                context.write(value,intWritable);
            }

        }
    }
}
