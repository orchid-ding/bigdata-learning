package bigdata.hadoop.mapreduces.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * @author dingchuangshi
 */
public class WholeInputStreamMain extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();

        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path(args[1]);
        if(fileSystem.exists(path)){
            fileSystem.delete(path,true);
        }

        Job job = Job.getInstance(conf,WholeInputStreamMain.class.getSimpleName());
        job.setJarByClass(WholeInputStreamMain.class);

        job.setInputFormatClass(WholeInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(SequenceFileMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        WholeInputFormat.addInputPath(job,new Path(args[0]));
        SequenceFileOutputFormat.setOutputPath(job,new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int exitCode = ToolRunner.run(new WholeInputStreamMain(),args);
        System.exit(exitCode);
    }

    /**
     * 自定义Mapper
     */
    public static class SequenceFileMapper  extends Mapper<NullWritable, BytesWritable, Text,BytesWritable> {

        private Text fileName;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            fileName = new Text(fileSplit.getPath().toString());
        }

        @Override
        protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(fileName,value);
        }
    }
}
