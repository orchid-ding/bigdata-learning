package bigdata.hbase.hbaseandmr.hdfs2hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author dingchuangshi
 */
public class HBaseMrMain extends Configured implements Tool
{
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","node01:2181,node02:2181,node03:2181");
        conf.set("zookeeper.znode.parent","/HBase");

        Job job = Job.getInstance(conf);
        job.setJarByClass(HBaseMrMain.class);
        job.setMapperClass(HBaseMapper.class);

        // 设置输入文件，输入路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(args[0]));

        // 设置输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置hbase
        TableMapReduceUtil.initTableReducerJob("scores",HBaseReduce.class,job);
        job.setNumReduceTasks(1);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int flag = ToolRunner.run(new HBaseMrMain(),args);
        System.exit(flag);
    }

    static class HBaseMapper extends Mapper<LongWritable, Text, Text,NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value,NullWritable.get());
        }
    }

    static class HBaseReduce extends TableReducer<Text, NullWritable, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            ImmutableBytesWritable keyWrite = new ImmutableBytesWritable();
            String[] splits = key.toString().split("\t");
            keyWrite.set(splits[0].getBytes());
            Put put = new Put(splits[0].getBytes());
            put.addColumn("info".getBytes(),"name".getBytes(),splits[1].getBytes());
            put.addColumn("info".getBytes(),"sex".getBytes(),splits[2].getBytes());
            put.addColumn("achievement".getBytes(),"language".getBytes(),splits[3].getBytes());
            put.addColumn("achievement".getBytes(),"mathematics".getBytes(),splits[4].getBytes());
            context.write(keyWrite,put);
        }
    }

}

