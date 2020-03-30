package top.kfly.example.addindex;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;

/**
 * @author dingchuangshi
 */
public class TransPortClient {

    private static TransportClient client;


    @BeforeEach
    public void init() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "myes").build();
        
        TransportAddress node01 = new TransportAddress(InetAddress.getByName("node01"), 9300);
        TransportAddress node02 = new TransportAddress(InetAddress.getByName("node02"), 9300);
        TransportAddress node03 = new TransportAddress(InetAddress.getByName("node03"), 9300);

        client = new PreBuiltTransportClient(settings)
                .addTransportAddresses(node01, node02, node03);
    }

    @Test
    public void createIndex() throws IOException {
        String json = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"travelying out Elasticsearch\"" +
                "}";

        HashMap<String, String> jsonMap = new HashMap<String, String>();
        jsonMap.put("name", "zhangsan");
        jsonMap.put("sex", "1");
        jsonMap.put("age", "18");
        jsonMap.put("address", "bj");


        Person person = new Person();
        person.setAge(18);
        person.setId(20);
        person.setName("张三丰");
        person.setAddress("武当山");
        person.setEmail("zhangsanfeng@163.com");
        person.setPhone("18588888888");
        person.setSex(1);
        String jsonPer = JSONObject.toJSONString(person);



        BulkRequestBuilder bulk = client.prepareBulk();

       bulk.add(client
                .prepareIndex("index23","ar","1")
                .setSource(json, XContentType.JSON)
               .setId("3")
                .setSource(jsonMap)
               .setId("2")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "lisi")
                        .field("age", "18")
                        .field("sex", "0")
                        .field("address", "bj")
                        .endObject())
               .setId("5")
                .setSource(jsonPer, XContentType.JSON));
        bulk.get();

    }

    @AfterEach
    public void close(){
        client.close();
    }


}
