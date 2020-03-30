package streaming.example.order.connect;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author dingchuangshi
 */
public class FileCustomSource implements SourceFunction<String> {

    private boolean isCancel = false;

    private BufferedReader bufferedReader;

    private FileReader fileReader;

    private String filePath;

    private Random random = new Random();

    public FileCustomSource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
       fileReader = new FileReader(filePath);
       bufferedReader = new BufferedReader(fileReader);
       String readLine = "";
        while (!isCancel && (readLine = bufferedReader.readLine()) != null){
            if(readLine != null){
                ctx.collect(readLine);
            }
            TimeUnit.MICROSECONDS.sleep(random.nextInt(500));
        }
        cancel();
    }

    @Override
    public void cancel() {
        isCancel = true;
        try{
            if(bufferedReader != null){
                bufferedReader.close();
            }
            if(fileReader != null){
                fileReader.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
