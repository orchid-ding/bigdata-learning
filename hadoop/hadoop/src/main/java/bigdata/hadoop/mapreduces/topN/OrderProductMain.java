package bigdata.hadoop.mapreduces.topN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author dingchuangshi
 */
public class OrderProductMain extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        // 开启日志
        BasicConfigurator.configure();
        int exitCode = ToolRunner.run(new OrderProductMain(),args);
        System.exit(exitCode);
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // 清空out路径
        FileSystem fileSystem = FileSystem.get(conf);
        Path  out = new Path(args[1]);
        if(fileSystem.exists(out)){
            fileSystem.delete(out,true);
        }
        // job
        Job job = Job.getInstance(conf,OrderProductMain.class.getSimpleName());

        job.setJarByClass(OrderProductMain.class);

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReduce.class);
        job.setGroupingComparatorClass(OrderGroupBean.class);


        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setPartitionerClass(OrderHashPartition.class);
//        job.setGroupingComparatorClass(OrderGroupBean.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static class OrderReduce extends Reducer<OrderBean,DoubleWritable,Text,DoubleWritable>{
        private AtomicInteger integer = new AtomicInteger(0);
        @Override
        protected void reduce(OrderBean key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
//           integer.incrementAndGet();
//            System.out.println("12345"+integer.get());
//            Iterator<DoubleWritable> iterator = values.iterator();
//           int index = 0;
//           while (iterator.hasNext() && index < 2){
//               context.write(new Text(key.getUserId() + "\t" + new SimpleDateFormat("yyyy-MM").format(key.getDateTime())), iterator.next());
//               index ++;
//           }
            //求每个用户、每个月、消费金额最多的两笔多少钱
            int num = 0;
            for(DoubleWritable value: values) {
                if(num < 2) {
                    String keyOut = key.getUserId() + "  " + new SimpleDateFormat("yyyy-MM").format(key.getDateTime());
                    context.write(new Text(keyOut), value);
                    num++;
                } else {
                    break;
                }
            }
        }
    }
    /**
     * userid、datetime、title商品标题、unitPrice商品单价、purchaseNum购买量、productId商品ID
     * 自定义Mapper
     */
    public static class OrderMapper extends Mapper<LongWritable, Text,OrderBean, DoubleWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[]  fields= value.toString().split("\t");
            try {
                if(fields.length != 6){
                    return;
                }
                System.out.println(new SimpleDateFormat("yyyy-MM").format(new SimpleDateFormat("yyyy-MM").parse(fields[1])));
                OrderBean orderBean = new OrderBean(
                        fields[0],
                        new SimpleDateFormat("yyyy-MM").parse(fields[1]),
                        fields[2],
                        Double.valueOf(fields[3]),
                        Integer.valueOf(fields[4]),
                        fields[5]
                );
                context.write(orderBean,new DoubleWritable(orderBean.getPurchaseNum() * orderBean.getUnitPrice()));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * 自定义HashPartition
     */
    public static class OrderHashPartition extends Partitioner<OrderBean, DoubleWritable> {

        @Override
        public int getPartition(OrderBean key, DoubleWritable value, int numReduceTasks) {
            // 使用userId，进行自定义分区， 确保同一个用户的订单数据存放在同一分。
            return (key.getUserId().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }
}
