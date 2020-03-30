package config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;
import java.util.Properties;

/**
 * @author dingchuangshi
 */
public class CustomConfig implements Serializable {

    private  ParameterTool parameterTool;

    private String hostname;

    private int port;

    private String redisHostName;

    private int redisPort;

    /**
     * kafka param
     */
    private String topics;

    private String bootstrapServers;

    private String groupId;

    private String autoCommit;

    private String offsetReset;

    private String outPutPath;

    private String outPutErrPath;

    public CustomConfig(String[] args) {
        this.parameterTool = ParameterTool.fromArgs(args);
        init(parameterTool);
    }

    private void init(ParameterTool parameterTool) {
        hostname = parameterTool.get("hostname","localhost");
        port = parameterTool.getInt("port",8080);

        redisHostName = parameterTool.get("redisHostName","localhost");
        redisPort = parameterTool.getInt("redisPort",6379);

        bootstrapServers = parameterTool.get("bootStrapServer","localhost:9092");
        topics = parameterTool.get("topics","kfly");
        groupId = parameterTool.get("groupId",topics+ "_topics_consumer");
        autoCommit = parameterTool.get("autocommit","false");
        offsetReset = parameterTool.get("offsetReset","earliest");

        outPutPath = parameterTool.get("outPutPath","./data/output/info/" + getClass().getPackage().getName()) + "/";
        outPutErrPath = parameterTool.get("outPutErrPath","./data/output/error/" + getClass().getPackage().getName()) + "/";
    }

    public Properties getConsumerProperites(){
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers",this.getBootstrapServers());
        consumerProperties.put("group.id",this.getGroupId());
        consumerProperties.put("enable.auto.commit",this.getAutoCommit());
        consumerProperties.put("auto.offset.reset",this.getOffsetReset());
        return consumerProperties;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public String getRedisHostName() {
        return redisHostName;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public String getTopics() {
        return topics;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getAutoCommit() {
        return autoCommit;
    }

    public String getOffsetReset() {
        return offsetReset;
    }

    public String getOutPutPath() {
        return outPutPath;
    }

    public String getOutPutErrPath() {
        return outPutErrPath;
    }
}
