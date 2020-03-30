package bigdata.hadoop.mapreduces.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * @author dingchuangshi
 */
public class WholeOutPutFormat extends FileOutputFormat<Text, NullWritable> {

    /**
     * 两个输出文件;
     * good用于保存好评文件；其它评级保存到bad中
     * 根据实际情况修改path;node01及端口号8020
     */
    String bad = "/Users/dingchuangshi/IdeaProjects/java/bigdata.hadoop/doc/outputformat/ordercomment/bad/r.txt";
    String good = "/Users/dingchuangshi/IdeaProjects/java/bigdata.hadoop/doc/outputformat/ordercomment/good/r.txt";


    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataOutputStream goodOutPutStream = fileSystem.create(new Path(bad));
        FSDataOutputStream badOutPutStream = fileSystem.create(new Path(good));
        WholeRecordWrite recordWrite = new WholeRecordWrite(goodOutPutStream,badOutPutStream);
        return recordWrite;
    }

}
