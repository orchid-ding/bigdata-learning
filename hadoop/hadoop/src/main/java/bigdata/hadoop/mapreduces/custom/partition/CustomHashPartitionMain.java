package bigdata.hadoop.mapreduces.custom.partition;

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
 * @author dingchuangshi
 */
public class CustomHashPartitionMain{


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();
        Configuration configuration = new Configuration();

        // 删除输出文件
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path(args[1]);
        if(fileSystem.exists(path)){
            fileSystem.delete(path,true);
        }

        Job job = Job.getInstance(configuration,CustomHashPartitionMain.class.getSimpleName());

        job.setJarByClass(CustomHashPartitionMain.class);

        job.setMapperClass(CustomMapper.class);
        job.setReducerClass(CustomReduce.class);
        job.setCombinerClass(CustomReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(4);
        job.setPartitionerClass(CustomHashPartition.class);


        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
    }


}
