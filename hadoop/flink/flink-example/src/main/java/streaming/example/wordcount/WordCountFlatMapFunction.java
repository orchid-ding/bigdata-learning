package streaming.example.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author dingchuangshi
 */
public class WordCountFlatMapFunction implements FlatMapFunction<String, WordCountBean> {

    @Override
    public void flatMap(String lines, Collector<WordCountBean> out) throws Exception {
        String[] splitLines = lines.split(" ");
        for (String line: splitLines) {
            out.collect(WordCountBean.of(line,1));
        }
    }
}
