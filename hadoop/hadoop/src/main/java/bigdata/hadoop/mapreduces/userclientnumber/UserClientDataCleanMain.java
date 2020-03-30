package bigdata.hadoop.mapreduces.userclientnumber;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 *
 3.- 现有一批日志文件，日志来源于用户使用搜狗搜索引擎搜索新闻，并点击查看搜索结果过程；
 - 但是，日志中有一些记录损坏，现需要使用MapReduce来将这些**损坏记录**（如记录中少字段、多字段）从日志文件中删除，此过程就是传说中的**数据清洗**。
 - 并且在清洗时，要**统计**损坏的记录数。

 * @author dingchuangshi
 */
public class UserClientDataCleanMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 开启log4j
        BasicConfigurator.configure();
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf,"dataCleanSagoCount");
        job.setJarByClass(UserClientMain.class);

        job.setMapperClass(UserClientDataCleanMapper.class);
        job.setReducerClass(UserClientReduce.class);
        job.setCombinerClass(UserClientReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(4);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
    }
}
