package bigdata.hbase.hbaseandmr.bulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author dingchuangshi
 */
public class HBaseBulkLoadMain extends Configured implements Tool {

    private static String INPUT_STRING = "bigdata.hadoop.hdfs://node01:8020/HBase/kfly";

    private static String OUT_PUT_STRING = "bigdata.hadoop.hdfs://node01:8020/HBase/out_Hfile1112";

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        //设定绑定的zk集群
        conf.set("bigdata.hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
        conf.set("zookeeper.znode.parent","/HBase");

        conf.set("fs.defaultFS","bigdata.hadoop.hdfs://node01:8020");
        int flag = ToolRunner.run(conf,new HBaseBulkLoadMain(),args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(HBaseBulkLoadMain.class);

        FileInputFormat.addInputPath(job,new Path(INPUT_STRING));

        job.setMapperClass(BulkLoadMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("bulktable"));

        String a = conf.get("fs.defaultFS");
        HFileOutputFormat2.configureIncrementalLoad(
                job,table,
                connection.getRegionLocator(TableName.valueOf("bulktable")));

        // 输出文件类型
        job.setOutputFormatClass(HFileOutputFormat2.class);
        HFileOutputFormat2.setOutputPath(job,new Path(OUT_PUT_STRING));

        return job.waitForCompletion(true) ? 0 :1;
    }

    static class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            Put put = new Put(Bytes.toBytes(splits[0]));
            put.addColumn("kf".getBytes(),"name".getBytes(),splits[1].getBytes());
            put.addColumn("kf".getBytes(),"age".getBytes(),splits[2].getBytes());
            context.write(new ImmutableBytesWritable(splits[0].getBytes()),put);
        }
    }

}
