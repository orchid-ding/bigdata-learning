package bigdata.hadoop.mapreduces.custom.partition;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import java.util.HashMap;
import java.util.Map;

/**
 * 自定义分区器
 * @author dingchuangshi
 */
public class CustomHashPartition extends Partitioner<Text, IntWritable> {

    private static Map<String,Integer> dict = new HashMap<>();

    static {
        dict.put("my",0);
        dict.put("love",1);
        dict.put("is",2);
        dict.put("me",3);
    }


    /**
     * @param text
     * @param intWritable
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        System.err.println(text.toString());
        if(text == null || text.toString().isEmpty()){
            return 0;
        }
        return dict.get(text.toString());
    }
}
