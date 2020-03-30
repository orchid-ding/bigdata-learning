package bigdata.hbase.hbaseandmr.hbase2hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.management.ImmutableDescriptor;
import java.io.IOException;


/**
 * @author dingchuangshi
 */
public class HBaseMrMain extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        //设定绑定的zk集群
        conf.set("bigdata.hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
//        conf.set("zookeeper.znode.parent","/HBase");
//        conf.set("fs.defaultFS","bigdata.hadoop.hdfs://node01:8020");

        Job job = Job.getInstance(conf);
        job.setJarByClass(HBaseMrMain.class);

        TableMapReduceUtil.initTableMapperJob(
                TableName.valueOf("kfly"),
                new Scan(),
                HBaseReadMapper.class,
                Text.class,
                Put.class,
                job);

        TableMapReduceUtil.initTableReducerJob("kfly_copy",HBaseReducer.class,job);

        job.setNumReduceTasks(1);
        return job.waitForCompletion(true) ? 0: 1;
    }

    public static void main(String[] args) throws Exception {
        int flag = ToolRunner.run(new HBaseMrMain(),args);
        System.exit(flag);
    }


    static class HBaseReadMapper extends TableMapper<Text, Put> {

        /**
         * @param key bigdata.hbase 中的rowKey
         * @param value  bigdata.hbase Result
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            // 当前数据的key
            String rowKey = Bytes.toString(key.get());
            Text keyText = new Text(rowKey);
            // 组装value
            Put put = new Put(Bytes.toBytes(rowKey));
            Cell[] cells = value.rawCells();
            for (int i = 0; i < cells.length; i++) {
                String family_name = Bytes.toString(CellUtil.cloneFamily(cells[i]));
                if("kf".equals(family_name) || "ly".equals(family_name)){
                    String qualifier_name = Bytes.toString(CellUtil.cloneQualifier(cells[i]));
                    if("name".equals(qualifier_name) || "age".equals(qualifier_name)){
                        put.add(cells[i]);
                    }
                }
            }

            if(!put.isEmpty()){
                context.write(keyText,put);
            }
        }
    }

    static class HBaseReducer extends TableReducer<Text, Put, ImmutableDescriptor> {

        @Override
        protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
            for (Put put : values) {
                context.write(null,put);
            }
        }
    }

}