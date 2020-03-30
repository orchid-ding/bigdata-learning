package streaming.output.redis;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author dingchuangshi
 */
public class CustomRedisMapper implements RedisMapper<Tuple2<String,Long>> {

    private static FlinkJedisPoolConfig config = null;

    public FlinkJedisPoolConfig getConfig() {
        if(config == null){
            config = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();
        }
        return config;
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.LPUSH);
    }

    @Override
    public String getKeyFromData(Tuple2<String, Long> data) {
        return data.f0;
    }

    @Override
    public String getValueFromData(Tuple2<String, Long> data) {
        return data.f1 + "";
    }


}
