package bigdata.hadoop.mapreduces.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 自定义RecordReader
 * @author dingchuangshi
 */
public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable> {

    /**
     *   要读取的分分片
     */
    private FileSplit fileSplit;

    /**
     * 配置文件
     */
    private Configuration conf;

    /**
     * value
     */
    private BytesWritable value = new BytesWritable();

    /**
     * 是否已经读取
     */
    private boolean processed = false;


    /**
     * 初始化分片、配置文件
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) split;
        this.conf = context.getConfiguration();
    }


    /**
     * 读取分片文件数据
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!processed){
            // 如果未被读取，则读取数据
            byte[] contents = new byte[(int) fileSplit.getLength()];
            Path path = fileSplit.getPath();
            FileSystem fileSystem = path.getFileSystem(conf);
            FSDataInputStream inputStream = null;
            try{
                inputStream = fileSystem.open(path);
                IOUtils.readFully(inputStream,contents,0,contents.length);
                value.set(contents,0,contents.length);
                processed = true;
                return true;
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                IOUtils.closeStream(inputStream);
            }
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed ? 1.0f : 0f;
    }

    @Override
    public void close() throws IOException {

    }
}
