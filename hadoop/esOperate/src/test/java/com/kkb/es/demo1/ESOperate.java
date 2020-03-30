package com.kkb.es.demo1;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.sql.jdbc.EsDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.beans.Transient;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ESOperate {
    TransportClient client;

    /**
     * 初始化es的客户端
     */
    @BeforeEach
    public  void initClient() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "myes").build();

        client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("node01"), 9300));


    }


    /**
     * 添加索引
     * 获取客户端
     */
    @Test
    public void createIndex(){
        String json = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"travelying out Elasticsearch\"" +
                "}";
        IndexResponse indexResponse = client.prepareIndex("myindex1", "article", "1").setSource(json, XContentType.JSON).get();


    }


    @Test
    public void index2() throws Exception {
        HashMap<String, String> jsonMap = new HashMap<String, String>();
        jsonMap.put("name", "zhangsan");
        jsonMap.put("sex", "1");
        jsonMap.put("age", "18");
        jsonMap.put("address", "bj");
        IndexResponse indexResponse = client.prepareIndex("myindex1", "article", "2")
                .setSource(jsonMap)
                .get();
    }




    /**
     * 通过XContentBuilder来实现索引的创建
     * @throws IOException
     */
    @Test
    public void index3() throws IOException {
        IndexResponse indexResponse = client.prepareIndex("myindex1", "article", "3")
                .setSource(new XContentFactory().jsonBuilder()
                        .startObject()
                        .field("name", "lisi")
                        .field("age", "18")
                        .field("sex", "0")
                        .field("address", "bj")
                        .endObject())
                .get();

    }



    /**
     * 将java对象转换为json格式字符串进行创建索引
     */
    @Test
    public void objToIndex(){
        Person person = new Person();
        person.setAge(18);
        person.setId(20);
        person.setName("张三丰");
        person.setAddress("武当山");
        person.setEmail("zhangsanfeng@163.com");
        person.setPhone("18588888888");
        person.setSex(1);
        String json = JSONObject.toJSONString(person);
        System.out.println(json);
        client.prepareIndex("myindex1","article","32").setSource(json,XContentType.JSON).get();
        client.close();
    }



    /**
     * 批量创建索引
     * @throws IOException
     */
    @Test
    public void index4() throws IOException {
        BulkRequestBuilder bulk = client.prepareBulk();
        bulk.add(client.prepareIndex("myindex1", "article", "4")
                .setSource(new XContentFactory().jsonBuilder()
                        .startObject()
                        .field("name", "wangwu")
                        .field("age", "18")
                        .field("sex", "0")
                        .field("address", "bj")
                        .endObject()));
        bulk.add(client.prepareIndex("news", "article", "5")
                .setSource(new XContentFactory().jsonBuilder()
                        .startObject()
                        .field("name", "zhaoliu")
                        .field("age", "18")
                        .field("sex", "0")
                        .field("address", "bj")
                        .endObject()));
        BulkResponse bulkResponse = bulk.get();
        System.out.println(bulkResponse);
    }

    /**
     * 初始化一批数据到索引库当中去准备做查询使用
     * 注意这里初始化的时候，需要给我们的数据设置分词属性
     * @throws Exception
     */
    @Test
    public void createIndexBatch() throws Exception {
        Settings settings = Settings
                .builder()
                .put("cluster.name", "myes") //节点名称， 在es配置的时候设置
                //自动发现我们其他的es的服务器
                .put("client.transport.sniff", "true")
                .build();
        //创建客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("node01"), 9300));//以本机作为节点
        //创建映射
        XContentBuilder mapping = jsonBuilder()
                .startObject()
                .startObject("properties")
                //      .startObject("m_id").field("type","keyword").endObject()
                .startObject("id").field("type", "integer").endObject()
                .startObject("name").field("type", "text").field("analyzer", "ik_max_word").endObject()
                .startObject("age").field("type", "integer").endObject()
                .startObject("sex").field("type", "text").field("analyzer", "ik_max_word").endObject()
                .startObject("address").field("type", "text").field("analyzer", "ik_max_word").endObject()
                .startObject("phone").field("type", "text").endObject()
                .startObject("email").field("type", "text").endObject()
                .startObject("say").field("type", "text").field("analyzer", "ik_max_word").endObject()
                .endObject()
                .endObject();
        //pois：索引名   cxyword：类型名（可以自己定义）
        PutMappingRequest putmap = Requests.putMappingRequest("indexsearch").type("mysearch").source(mapping);
        //创建索引
        client.admin().indices().prepareCreate("indexsearch").execute().actionGet();
        //为索引添加映射
        client.admin().indices().putMapping(putmap).actionGet();


        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        Person lujunyi = new Person(2, "玉麒麟卢俊义", 28, 1, "水泊梁山", "17666666666", "lujunyi@163.com","hello world今天天气还不错");
        Person wuyong = new Person(3, "智多星吴用", 45, 1, "水泊梁山", "17666666666", "wuyong@163.com","行走四方，抱打不平");
        Person gongsunsheng = new Person(4, "入云龙公孙胜", 30, 1, "水泊梁山", "17666666666", "gongsunsheng@163.com","走一个");
        Person guansheng = new Person(5, "大刀关胜", 42, 1, "水泊梁山", "17666666666", "wusong@163.com","我的大刀已经饥渴难耐");
        Person linchong = new Person(6, "豹子头林冲", 18, 1, "水泊梁山", "17666666666", "linchong@163.com","梁山好汉");
        Person qinming = new Person(7, "霹雳火秦明", 28, 1, "水泊梁山", "17666666666", "qinming@163.com","不太了解");
        Person huyanzhuo = new Person(8, "双鞭呼延灼", 25, 1, "水泊梁山", "17666666666", "huyanzhuo@163.com","不是很熟悉");
        Person huarong = new Person(9, "小李广花荣", 50, 1, "水泊梁山", "17666666666", "huarong@163.com","打酱油的");
        Person chaijin = new Person(10, "小旋风柴进", 32, 1, "水泊梁山", "17666666666", "chaijin@163.com","吓唬人的");
        Person zhisheng = new Person(13, "花和尚鲁智深", 15, 1, "水泊梁山", "17666666666", "luzhisheng@163.com","倒拔杨垂柳");
        Person wusong = new Person(14, "行者武松", 28, 1, "水泊梁山", "17666666666", "wusong@163.com","二营长。。。。。。");

        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "1")
                .setSource(JSONObject.toJSONString(lujunyi), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "2")
                .setSource(JSONObject.toJSONString(wuyong), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "3")
                .setSource(JSONObject.toJSONString(gongsunsheng), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "4")
                .setSource(JSONObject.toJSONString(guansheng), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "5")
                .setSource(JSONObject.toJSONString(linchong), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "6")
                .setSource(JSONObject.toJSONString(qinming), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "7")
                .setSource(JSONObject.toJSONString(huyanzhuo), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "8")
                .setSource(JSONObject.toJSONString(huarong), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "9")
                .setSource(JSONObject.toJSONString(chaijin), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "10")
                .setSource(JSONObject.toJSONString(zhisheng), XContentType.JSON)
        );
        bulkRequestBuilder.add(client.prepareIndex("indexsearch", "mysearch", "11")
                .setSource(JSONObject.toJSONString(wusong), XContentType.JSON)
        );

        bulkRequestBuilder.get();
        client.close();

    }


    @Test
    public void getById(){
        GetRequestBuilder getRequestBuilder = client.prepareGet("indexsearch", "mysearch", "1");
        //返回的数据全部都封装在GetResponse里面了
        GetResponse documentFields = getRequestBuilder.get();

        String index = documentFields.getIndex();
        String type = documentFields.getType();
        String id = documentFields.getId();
        String sourceAsString = documentFields.getSourceAsString();
        System.out.println("index :" + index + "type:" + type + "id : " + id +"content:"+ sourceAsString);


    }


    /**
     * 查询索引库当中所有的数据
     */
    @Test
    public void queryAll(){
        //查询所有的数据，响应的结果是SearchResponse
        SearchResponse searchResponse = client.prepareSearch("indexsearch").setTypes("mysearch").setQuery(QueryBuilders.matchAllQuery()).get();
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }


    /**
     * 查找索引库当中年龄为18到28的所有人
     */
    @Test
    public void rangeQuery(){
        SearchResponse searchResponse = client.prepareSearch("indexsearch").setTypes("mysearch")
                .setQuery(QueryBuilders.rangeQuery("age").gt(18).lte(28))
                .get();
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }


    /**
     * 词条查询
     */
    @Test
    public void termQuery(){
        SearchResponse searchResponse = client.prepareSearch("indexsearch").setTypes("mysearch")
                .setQuery(QueryBuilders.termQuery("say", "熟悉")).get();
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }


    /**
     * 模糊查询，自动帮我门纠正些错误的英文单词
     */
    @Test
    public void fuzzyQuery(){

        SearchResponse searchResponse = client.prepareSearch("indexsearch").setTypes("mysearch")
                .setQuery(QueryBuilders.fuzzyQuery("say", "helol").fuzziness(Fuzziness.TWO)).get();
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }


    /**
     * 通配符查询
     * * 匹配任意多个字符
     * ？ 仅仅匹配一个字符
     *  *ello
     */
    @Test
    public void wildCardQuery(){
        SearchResponse searchResponse = client.prepareSearch("indexsearch").setTypes("mysearch")
                .setQuery(QueryBuilders.wildcardQuery("say", "hel*")).get();
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }

    }


    /**
     * 查询年龄是18到28范围内且性别是男性的，或者id范围在10到13范围内的
     */
    @Test
    public void booleanQuery(){
        RangeQueryBuilder age = QueryBuilders.rangeQuery("age").gt(18).lte(28);
        TermQueryBuilder sex = QueryBuilders.termQuery("sex", "1");
        RangeQueryBuilder id = QueryBuilders.rangeQuery("id").gt(10).lt(13);

        SearchResponse searchResponse = client.prepareSearch("indexsearch").setTypes("mysearch")
                .setQuery(QueryBuilders.boolQuery().should(id).should(QueryBuilders.boolQuery().must(sex).must(age
                ))).get();

        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }

    }


    /**
     * 分页查询
     */
    @Test
    public void pageIndex(){
        int pageSize = 5;
        int pageNum = 2;
        int startNum = (pageNum - 1) * pageSize;
        SearchResponse searchResponse = client.prepareSearch("indexsearch").setTypes("mysearch")
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort("id", SortOrder.ASC)
                .setFrom(startNum)
                .setSize(pageSize)
                .get();
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }


    /**
     * 高亮显示
     */
    @Test
    public void highlighting(){
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("indexsearch").setTypes("mysearch")
                .setQuery(QueryBuilders.termQuery("say", "hello"));

        //高亮显示的关键类
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("say").preTags("<font style='color:red'>").postTags("</font>");

        SearchResponse searchResponse = searchRequestBuilder.highlighter(highlightBuilder).get();

        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            Text[] says = hit.getHighlightFields().get("say").getFragments();
            for (Text say : says) {
                System.out.println(say);
            }
        }


    }




    /**
     * 更新索引
     * 根据数据id来进行更新索引
     */
    @Test
    public void updateIndex(){
        Person guansheng = new Person(5, "宋江", 88, 0, "水泊梁山", "17666666666", "wusong@kkb.com","及时雨宋江");
        client.prepareUpdate().setIndex("indexsearch").setType("mysearch").setId("5")
                .setDoc(JSONObject.toJSONString(guansheng),XContentType.JSON)
                .get();
        client.close();
    }




    /**
     * 按照id进行删除数据
     */
    @Test
    public void deleteById(){
        DeleteResponse deleteResponse = client.prepareDelete("indexsearch", "mysearch", "14").get();
        client.close();
    }


    /*
按照条件进行删除
 */
    @Test
    public void  deleteByQuery(){

        BulkByScrollResponse bulkByScrollResponse = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.rangeQuery("id").gt(2).lt(4))
                .source("indexsearch")
                .get();
        long deleted = bulkByScrollResponse.getDeleted();
        System.out.println(deleted);

    }


    /**
     * 删除索引
     * 删除整个索引库
     */
    @Test
    public  void  deleteIndex(){
         client.admin().indices().prepareDelete("indexsearch").execute().actionGet();
        client.close();
    }





    /**
     * 批量添加数据
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void addIndexDatas() throws IOException, ExecutionException, InterruptedException {
        //获取settings
        //配置es集群的名字
        Settings settings = Settings.builder().put("cluster.name", "myes").build();
        //获取客户端
        TransportAddress transportAddress = new TransportAddress(InetAddress.getByName("node01"), 9300);

        TransportAddress transportAddress2 = new TransportAddress(InetAddress.getByName("node02"), 9300);

        TransportAddress transportAddress3 = new TransportAddress(InetAddress.getByName("node03"), 9300);
        //获取client客户端
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(transportAddress).addTransportAddress(transportAddress2).addTransportAddress(transportAddress3);

        /**
         * 创建索引
         * */
        client.admin().indices().prepareCreate("player").get();
        //构建json的数据格式，创建映射
        XContentBuilder mappingBuilder = jsonBuilder()
                .startObject()
                .startObject("player")
                .startObject("properties")
                .startObject("name").field("type","text").field("index", "true").field("fielddata","true").endObject()
                .startObject("age").field("type","integer").endObject()
                .startObject("salary").field("type","integer").endObject()
                .startObject("team").field("type","text").field("index", "true").field("fielddata","true").endObject()
                .startObject("position").field("type","text").field("index", "true").field("fielddata","true").endObject()
                .endObject()
                .endObject()
                .endObject();
        PutMappingRequest request = Requests.putMappingRequest("player")
                .type("player")
                .source(mappingBuilder);
        client.admin().indices().putMapping(request).get();


        //批量添加数据开始

        BulkRequestBuilder bulkRequest = client.prepareBulk();

// either use client#prepare, or use Requests# to directly build index/delete requests
        bulkRequest.add(client.prepareIndex("player", "player", "1")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("name", "郭德纲")
                        .field("age", 33)
                        .field("salary",3000)
                        .field("team" , "cav")
                        .field("position" , "sf")
                        .endObject()
                )
        );
        bulkRequest.add(client.prepareIndex("player", "player", "2")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("name", "于谦")
                        .field("age", 25)
                        .field("salary",2000)
                        .field("team" , "cav")
                        .field("position" , "pg")
                        .endObject()
                )
        );
        bulkRequest.add(client.prepareIndex("player", "player", "3")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("name", "岳云鹏")
                        .field("age", 29)
                        .field("salary",1000)
                        .field("team" , "war")
                        .field("position" , "pg")
                        .endObject()
                )
        );
        bulkRequest.add(client.prepareIndex("player", "player", "4")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("name", "孙越")
                        .field("age", 26)
                        .field("salary",2000)
                        .field("team" , "war")
                        .field("position" , "sg")
                        .endObject()
                )
        );
        bulkRequest.add(client.prepareIndex("player", "player", "5")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("name", "张云雷")
                        .field("age", 26)
                        .field("salary",2000)
                        .field("team" , "war")
                        .field("position" , "pf")
                        .endObject()
                )
        );
        bulkRequest.add(client.prepareIndex("player", "player", "6")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("name", "爱迪生")
                        .field("age", 40)
                        .field("salary",1000)
                        .field("team" , "tim")
                        .field("position" , "pf")
                        .endObject()
                )
        );
        bulkRequest.add(client.prepareIndex("player", "player", "7")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("name", "牛顿")
                        .field("age", 21)
                        .field("salary",500)
                        .field("team" , "tim")
                        .field("position" , "c")
                        .endObject()
                )
        );
        bulkRequest.add(client.prepareIndex("player", "player", "4")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("name", "爱因斯坦")
                        .field("age", 21)
                        .field("salary",300)
                        .field("team" , "tim")
                        .field("position" , "sg")
                        .endObject()
                )
        );
        bulkRequest.add(client.prepareIndex("player", "player", "8")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("name", "特斯拉")
                        .field("age", 20)
                        .field("salary",500)
                        .field("team" , "tim")
                        .field("position" , "sf")
                        .endObject()
                )
        );


        BulkResponse bulkResponse = bulkRequest.get();
        client.close();
    }


    /**
     * 需求一：统计每个球队当中球员的数量
     * sql语句实现
     * select team, count(*) as player_count from player group by team;
     */
    @Test
    public void countPlayer(){
        SearchRequestBuilder builder = client.prepareSearch("player").setTypes("player");
        //创建聚合条件，指定我们按照球队进行分组
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("player_count").field("team");
         builder.addAggregation(termsAggregationBuilder);
        SearchResponse searchResponse = builder.get();
        //获取聚合的结果
        Aggregations aggregations = searchResponse.getAggregations();
        for (Aggregation aggregation : aggregations) {
            StringTerms stringTerms = (StringTerms) aggregation;
            List<StringTerms.Bucket> buckets = stringTerms.getBuckets();
            for (StringTerms.Bucket bucket : buckets) {
                System.out.println(bucket.getKey());
                System.out.println(bucket.getDocCount());
            }

        }
    }

    /**
     * 需求二：统计每个球队中每个位置的球员数量
     * sql语句实现
     * select team, position, count(*) as pos_count from player group by team, position;
     */
    @Test
    public void countPlayerAndPosition(){
        SearchRequestBuilder builder = client.prepareSearch("player").setTypes("player");
        TermsAggregationBuilder team = AggregationBuilders.terms("player_count").field("team");
        TermsAggregationBuilder position = AggregationBuilders.terms("posititon_count").field("position");
        //指定多个分组条件的先后顺序关系
        team.subAggregation(position);
        SearchResponse searchResponse = builder.addAggregation(team).addAggregation(position).get();
        //获取聚合结果
        Aggregations aggregations = searchResponse.getAggregations();
        for (Aggregation aggregation : aggregations) {
            // System.out.println(aggregation.toString());
            StringTerms stringTerms = (StringTerms) aggregation;
            List<StringTerms.Bucket> buckets = stringTerms.getBuckets();
            for (StringTerms.Bucket bucket : buckets) {
                long docCount = bucket.getDocCount();
                Object key = bucket.getKey();
                System.out.println("当前队伍名称为" +  key + "该队伍下有"+docCount + "个球员");

                Aggregation posititon_count = bucket.getAggregations().get("posititon_count");
                if(null != posititon_count){
                    StringTerms positionTrem = (StringTerms) posititon_count;
                    List<StringTerms.Bucket> buckets1 = positionTrem.getBuckets();
                    for (StringTerms.Bucket bucket1 : buckets1) {
                        Object key1 = bucket1.getKey();
                        long docCount1 = bucket1.getDocCount();
                        System.out.println("该队伍下面的位置为" +  key1+"该位置下有" +  docCount1 +"人");
                    }
                }
            }
        }


    }


    /**
     * 需求三：分组求各种值
     * 计算每个球队年龄最大值
     * select team, max(age) as max_age from player group by team;
     */
    @Test
    public void maxAgeTeam(){

        SearchRequestBuilder builder = client.prepareSearch("player").setTypes("player");
        TermsAggregationBuilder team = AggregationBuilders.terms("team_group").field("team");

        MaxAggregationBuilder age = AggregationBuilders.max("max_age").field("age");
        //指明我们的球队以及年龄的先后顺序关系
        team.subAggregation(age);

        SearchResponse searchResponse = builder.addAggregation(team).get();
        Aggregations aggregations = searchResponse.getAggregations();
        for (Aggregation aggregation : aggregations) {
            StringTerms stringTerms = (StringTerms) aggregation;
            List<StringTerms.Bucket> buckets = stringTerms.getBuckets();
            for (StringTerms.Bucket bucket : buckets) {
                Aggregation max_age = bucket.getAggregations().get("max_age");
                System.out.println(max_age.toString());
            }
        }


    }


    /**
     * 求每个球队当中年龄最小的值
     */
    @Test
    public void minAgeGroup(){

        SearchRequestBuilder builder = client.prepareSearch("player").setTypes("player");

        TermsAggregationBuilder team = AggregationBuilders.terms("team_count").field("team");

        MinAggregationBuilder age = AggregationBuilders.min("min_age").field("age");

        TermsAggregationBuilder termAggregation = team.subAggregation(age);

        SearchResponse searchResponse = builder.addAggregation(termAggregation).get();
        Aggregations aggregations = searchResponse.getAggregations();
        for (Aggregation aggregation : aggregations) {
            System.out.println(aggregation.toString());

            StringTerms stringTerms = (StringTerms) aggregation;
            List<StringTerms.Bucket> buckets = stringTerms.getBuckets();
            for (StringTerms.Bucket bucket : buckets) {
                Aggregations aggregations1 = bucket.getAggregations();
                for (Aggregation aggregation1 : aggregations1) {
                    System.out.println(aggregation1.toString());
                }
            }
        }



    }


    /**
     * 求平均值
     */
    @Test
    public  void avgAge(){
        SearchRequestBuilder builder = client.prepareSearch("player").setTypes("player");

        TermsAggregationBuilder team_field = AggregationBuilders.terms("player_count").field("team");


        AvgAggregationBuilder age_avg = AggregationBuilders.avg("age_avg").field("age");

        team_field.subAggregation(age_avg);

        SearchResponse searchResponse = builder.addAggregation(team_field).get();

        Aggregations aggregations = searchResponse.getAggregations();
        for (Aggregation aggregation : aggregations) {
            System.out.println(aggregation.toString());
            StringTerms stringTerms = (StringTerms) aggregation;
        }


    }


    /**
     * 需求六：分组求和
     * 计算每个球队球员的平均年龄，同时又要计算总年薪
     */
    @Test
    public void groupCount(){
        SearchRequestBuilder builder = client.prepareSearch("player").setTypes("player");

        TermsAggregationBuilder team_group = AggregationBuilders.terms("team_group").field("team");

        AvgAggregationBuilder avg_age = AggregationBuilders.avg("avg_age").field("age");

        SumAggregationBuilder sumMoney = AggregationBuilders.sum("sum_money").field("salary");


        TermsAggregationBuilder termsAggregationBuilder = team_group.subAggregation(avg_age).subAggregation(sumMoney);

        SearchResponse searchResponse = builder.addAggregation(termsAggregationBuilder).get();
        Aggregations aggregations = searchResponse.getAggregations();
        for (Aggregation aggregation : aggregations) {
            System.out.println(aggregation.toString());
        }




    }


    /**
     * 按照总年薪进行倒序的排序
     */
    @Test
    public void sortCount(){
        SearchRequestBuilder builder = client.prepareSearch("player").setTypes("player");
        TermsAggregationBuilder teamGroup = AggregationBuilders.terms("team_group").field("team").order(BucketOrder.count(true));
        SumAggregationBuilder sumSalary = AggregationBuilders.sum("sum_salary").field("salary");
        TermsAggregationBuilder termsAggregationBuilder = teamGroup.subAggregation(sumSalary);
        SearchResponse searchResponse = builder.addAggregation(termsAggregationBuilder).get();
        Map<String, Aggregation> stringAggregationMap = searchResponse.getAggregations().asMap();
        System.out.println(stringAggregationMap.toString());
        Aggregations aggregations = searchResponse.getAggregations();
        for (Aggregation aggregation : aggregations) {
            System.out.println(aggregation.toString());
        }


    }


    /**
     * 基于地理位置的搜索
     */
    @Test
    public  void locationQuery() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", "myes").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("node01"), 9300));

        /**
         * 基于矩形范围的数据搜索
         *          40.0519526142,116.4178513254
         *
         *          40.0385828363,116.4465266673
         */
        SearchResponse searchResponse = client.prepareSearch("platform_foreign_website")
                .setTypes("store")
                .setQuery(QueryBuilders.geoBoundingBoxQuery("location").setCorners(40.0519526142, 116.4178513254, 40.0385828363, 116.4465266673))
                .get();
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }

        System.out.println("=========================分割线============================");
        /**
         *
         *       40.0519526142,116.4178513254
         *
         *      40.0519526142,116.4178513254
         *
         *      40.0385828363,116.4465266673
         *
         *
         *
         *      40.0519526142,116.4178513254
         *
         *      40.0503813013,116.4562592119
         *
         *      40.0385828363,116.4465266673
         */
        List<GeoPoint> points = new ArrayList<GeoPoint>();
        points.add(new GeoPoint(40.0519526142,116.4178513254));
        points.add(new GeoPoint(40.0503813013, 116.4562592119));
        points.add(new GeoPoint(40.0385828363, 116.4465266673));


        SearchResponse searchResponse1 = client.prepareSearch("platform_foreign_website")
                .setTypes("store")
                .setQuery(QueryBuilders.geoPolygonQuery("location", points))
                .get();

        SearchHit[] hits1 = searchResponse1.getHits().getHits();
        for (SearchHit documentFields : hits1) {
            System.out.println(documentFields.getSourceAsString());
        }


        System.out.println("====================================================");
        /**
         * 以当前的点为中心，搜索落在半径范围内200公里的经纬度坐标点
         *40.0488115498,116.4320345091
         */
        SearchResponse searchResponse2 = client.prepareSearch("platform_foreign_website")
                .setTypes("store")
                .setQuery(QueryBuilders.geoDistanceQuery("location").point(40.0488115498, 116.4320345091).distance(100, DistanceUnit.KILOMETERS))
                .get();

        SearchHit[] hits2 = searchResponse2.getHits().getHits();
        for (SearchHit documentFields : hits2) {
            System.out.println(documentFields.getSourceAsString());
        }
    }



    @Test
    public void esJdbc() throws SQLException {
        EsDataSource dataSource = new EsDataSource();
        String address = "jdbc:es://http://node01:9200" ;
        dataSource.setUrl(address);
        Properties connectionProperties = new Properties();
        dataSource.setProperties(connectionProperties);
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from library");
        while(resultSet.next()){
            String string = resultSet.getString(0);
            String string1 = resultSet.getString(1);
            int anInt = resultSet.getInt(2);
            String string2 = resultSet.getString(4);
            System.out.println(string + "\t" +  string1 + "\t" +  anInt + "\t" + string2);
        }
        connection.close();
    }



    @AfterEach
    public void closeClient(){
        client.close();
    }



}
