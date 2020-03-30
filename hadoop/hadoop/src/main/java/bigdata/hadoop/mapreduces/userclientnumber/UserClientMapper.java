package bigdata.hadoop.mapreduces.userclientnumber;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * mapper
 * @author dingchuangshi
 */
public class UserClientMapper  extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     *  intWrite  初始化key，数量为1
     *  intWrite  初始化key，数量为1
     *  intWrite  初始化key，数量为1
     */
    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 自定义计数器，获取错误数据
        Counter counter = context.getCounter("DataClean","sougouSRFlogs");
        Text keyValue = new Text();
        // 切分value 获取搜狗输入参数
        String[] logs = value.toString().split("\t");

        if(logs.length != 6){
            counter.increment(1);
        }else {
            keyValue.set(logs[1]);
            context.write(new Text(keyValue),one);
        }
    }
}
