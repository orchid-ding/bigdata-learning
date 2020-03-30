package bigdata.hadoop.mapreduces.workcount.core;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * 统计单词量 mapper
 * 对数据进行切分
 * @author dingchuangshi
 */
public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        IntWritable writable = new IntWritable(1);
        Text keyText = new Text();
        // 对单行数据进行切分，得到单个词
        String[] mapKeys = value.toString().split(" ");

        // 将切分后的数据保存，数量为1
        for (int i = 0; i < mapKeys.length; i++) {
            keyText.set(mapKeys[i]);
            context.write(keyText,writable);
        }

    }
}
