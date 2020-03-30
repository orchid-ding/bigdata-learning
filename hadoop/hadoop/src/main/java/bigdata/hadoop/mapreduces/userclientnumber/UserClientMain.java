package bigdata.hadoop.mapreduces.userclientnumber;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;

/**
 * 2.使用MR编程，统计sogou日志数据中，每个用户搜索的次数；结果写入HDFS
 * @author dingchuangshi
 */
public class UserClientMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 开启log4j
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        // 设置压缩
        conf.set("mapreduce.map.output.compress","true");
        conf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.BZip2Codec");
        conf.set("mapreduce.output.fileoutputformat.compress","true");
        conf.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.BZip2Codec");

        //删除已生成文件
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path(args[1]);
        if(fileSystem.exists(path)){
            fileSystem.delete(path,true);
        }

        Job job = Job.getInstance(conf,"dataCleanSagoCount");
        job.setJarByClass(UserClientMain.class);

        job.setMapperClass(UserClientMapper.class);
        job.setReducerClass(UserClientReduce.class);
        job.setCombinerClass(UserClientReduce.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);

    }
}
