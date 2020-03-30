package top.kfly.core;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author dingchuangshi
 */
public class CustomFlatCoMapFunction implements CoFlatMapFunction<String, Map<String, String>, String> {

    private Logger logger = LoggerFactory.getLogger(CustomFlatCoMapFunction.class);

    private Map<String,String> redisAreasData = new HashMap<>();

    @Override
    public void flatMap1(String value, Collector<String> out) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(value);
        String dt = jsonObject.getString("dt");
        String countryCode = jsonObject.getString("countryCode");
        // 可以根据countryCode获取大区的名字
        String area = redisAreasData.get(countryCode);
        JSONArray data = jsonObject.getJSONArray("data");
        for (int i = 0; i < data.size(); i++) {
            JSONObject dataObject = data.getJSONObject(i);
            System.out.println("大区："+area);
            dataObject.put("dt", dt);
            dataObject.put("area", area);
            // 下游获取到数据的时候，也就是一个json格式的数据
            out.collect(dataObject.toJSONString());
        }
    }

    @Override
    public void flatMap2(Map<String, String> value, Collector<String> out) throws Exception {
        if(logger.isDebugEnabled()){
            logger.debug(value.toString());
        }
        redisAreasData = value;
    }
}
