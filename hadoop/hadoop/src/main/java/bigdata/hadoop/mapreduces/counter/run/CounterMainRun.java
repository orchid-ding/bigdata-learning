package bigdata.hadoop.mapreduces.counter.run;

import bigdata.hadoop.mapreduces.counter.core.CounterMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * @author dingchuangshi
 */
public class CounterMainRun {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Counter");

        job.setJarByClass(CounterMainRun.class);
        job.setNumReduceTasks(0);

        job.setMapperClass(CounterMapper.class);


        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
    }
}
