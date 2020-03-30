package top.kfly.source;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import top.kfly.config.CustomConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * hset areas AREA_US US
 * hset areas AREA_CT TW,HK
 * hset areas AREA_AR PK,KW,SA
 * hset areas AREA_IN IN
 * @author dingchuangshi
 */
public class CustomRedisSource implements SourceFunction<Map<String,String>> {

    private Logger logger= LoggerFactory.getLogger(CustomRedisSource.class);

    private boolean isRunning=true;

    private CustomConfig customConfig;
    private Jedis jedis;

    public CustomRedisSource(CustomConfig customConfig) {
        this.customConfig = customConfig;
    }

    @Override
    public void run(SourceContext<Map<String,String>> ctx) throws Exception {
        this.jedis = new Jedis(customConfig.getRedisHostName(),customConfig.getRedisPort());
        Map<String,String> map = new HashMap<>();
        while (isRunning){
            try{
                map.clear();
                Map<String, String> areas = jedis.hgetAll("areas");
                System.out.println(areas);
                for(Map.Entry<String,String> entry: areas.entrySet()){
                    String area = entry.getKey();
                    String value = entry.getValue();
                    String[] fields = value.split(",");
                    for(String country:fields){
                        map.put(country,area);
                    }

                }
                if(map.size() > 0 ){
                    ctx.collect(map);
                }
                Thread.sleep(60000);
            }catch (Exception e){
                logger.error("数据源异常",e.getCause());
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        if(jedis != null){
            jedis.close();
        }
    }
}
