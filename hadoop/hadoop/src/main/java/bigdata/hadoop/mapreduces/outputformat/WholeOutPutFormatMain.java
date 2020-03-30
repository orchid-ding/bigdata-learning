package bigdata.hadoop.mapreduces.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * @author dingchuangshi
 */
public class WholeOutPutFormatMain extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // 清除out文件
        FileSystem fileSystem = FileSystem.get(conf);
        Path outPath = new Path(args[1]);
        if(fileSystem.exists(outPath)){
            fileSystem.delete(outPath,true);
        }

        Job job = Job.getInstance(conf,WholeOutPutFormatMain.class.getSimpleName());

        job.setJarByClass(WholeOutPutFormatMain.class);

        job.setMapperClass(WholeOutPutFormatMapper.class);
        job.setOutputFormatClass(WholeOutPutFormat.class);
        WholeOutPutFormat.setOutputPath(job,new Path(args[1]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int exitCOde = ToolRunner.run(new WholeOutPutFormatMain(),args);
        System.exit(exitCOde);
    }

    public static class WholeOutPutFormatMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value,NullWritable.get());
        }
    }
}
