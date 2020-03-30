package bigdata.hadoop.mapreduces.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author dingchuangshi
 */
public class WholeRecordWrite extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream goodOutPutStream;

    private FSDataOutputStream badOutPutStream;

    /**
     *
     * @param goodOutPutStream
     * @param badOutPutStream
     */
    public WholeRecordWrite(FSDataOutputStream goodOutPutStream,FSDataOutputStream badOutPutStream){
        this.goodOutPutStream = goodOutPutStream;
        this.badOutPutStream = badOutPutStream;
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        // 如果是好评，写入goodOutPutStream
        if(COMMENT.equals(key.toString().split(TABLE)[INDEX_COMMENT])){
            goodOutPutStream.write(key.toString().getBytes());
            goodOutPutStream.write("\r\t".getBytes());
        }else {
            badOutPutStream.write(key.toString().getBytes());
            badOutPutStream.write("\r\t".getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(goodOutPutStream);
        IOUtils.closeStream(badOutPutStream);
    }

    private static final String COMMENT = "0";
    private static final String TABLE = "\t";
    private static final int INDEX_COMMENT = 9;

}
