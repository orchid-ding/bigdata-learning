package bigdata.hadoop.mapreduces.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * @author dingchuangshi
 */
public class SecondarySort  extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int exitCode = ToolRunner.run(new SecondarySort(),args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path(args[1]);
        if(fileSystem.exists(path)){
            fileSystem.delete(path,true);
        }

        Job job = Job.getInstance(conf,SecondarySort.class.getSimpleName());

        job.setJarByClass(SecondarySort.class);

        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReduce.class);

        job.setOutputKeyClass(People.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,path);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 自定义Mapper
     */
    public static class SecondarySortMapper extends Mapper<LongWritable, Text,People, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] peopleStr = value.toString().split("\t");
            People people = new People();
            people.setName(peopleStr[0]);
            people.setAge(Integer.valueOf(peopleStr[1]));
            people.setSalary(Integer.valueOf(peopleStr[2]));
            context.write(people,NullWritable.get());
        }
    }

    /**
     * 自定义reduce
     */
    public static class SecondarySortReduce extends Reducer<People,NullWritable,People,NullWritable>{
        @Override
        protected void reduce(People key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }
}
