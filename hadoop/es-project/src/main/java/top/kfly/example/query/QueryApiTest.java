package top.kfly.example.query;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import top.kfly.example.addindex.Person;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author dingchuangshi
 */
public class QueryApiTest {

    private static TransportClient client;

    public void init() throws UnknownHostException {

        Settings settings = Settings.builder()
                .put("cluster.name", "myes")
                .put("client.transport.sniff", "true").build();

        TransportAddress address = new TransportAddress(InetAddress.getByName("node01"),9300);

        client = new PreBuiltTransportClient(settings).addTransportAddress(address);
    }


    public void queryById(){
        /**
         * index:
         * type:
         * id
         */
        GetRequestBuilder requestBuilder = client.prepareGet("indexsearch", "mysearch", "1");

        GetResponse getResponse = requestBuilder.get();
        print(getResponse);
    }



    public void queryByRange(){
        SearchResponse searchResponse = client.prepareSearch("indexsearch")
                .setTypes("mysearch")
                .setQuery(new RangeQueryBuilder("age").lt(30).gt(17))
                .get();
        print(searchResponse);
    }
    public void queryAll(){
        SearchResponse searchResponse = client
                .prepareSearch("indexsearch")
                .setTypes("mysearch")
                .setQuery(new MatchAllQueryBuilder())
                .get();
        print(searchResponse);
    }

    /**
     * fuzzyQuery表示英文单词的最大可纠正次数，最大可以自动纠正两次
     */
    public void queryByFuzzy(){
        SearchResponse searchResponse = client
                .prepareSearch("indexsearch")
                .setTypes("mysearch")
                .setQuery(QueryBuilders.fuzzyQuery("say","helOL").fuzziness(Fuzziness.TWO))
                .get();
        print(searchResponse);
    }

    public void queryByWildCard(){
        SearchResponse searchResponse = client.prepareSearch("indexsearch")
                .setTypes("mysearch")
                .setQuery(QueryBuilders.wildcardQuery("say", "hel*"))
                .get();

        print(searchResponse);
    }

    /**
     * 多条件组合查询 boolQuery
     * 查询年龄是18到28范围内且性别是男性的，或者id范围在10到13范围内的
     */
    public void queryBool(){
        RangeQueryBuilder age = QueryBuilders.rangeQuery("age").gt(17).lt(29);
        TermQueryBuilder sex = QueryBuilders.termQuery("sex", "1");
        RangeQueryBuilder id = QueryBuilders.rangeQuery("id").gt("9").lt("15");


        BoolQueryBuilder should = QueryBuilders.boolQuery().
                should(id).
                should(
                        QueryBuilders.boolQuery()
                                .must(age)
                                .must(sex)
                );

        SearchResponse searchResponse = client.prepareSearch("indexsearch")
                .setTypes("mysearch")
                .setQuery(should)
                .get();
        print(searchResponse);
    }

    public void getPageIndex(){
        SearchResponse searchResponse = client.prepareSearch("indexsearch")
                .setTypes("mysearch")
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort("id", SortOrder.ASC)
                .setFrom(2)
                .setSize(5)
                .get();
        print(searchResponse);
    }

    public void highLight(){
        SearchResponse searchResponse = client.prepareSearch("indexsearch")
                .setTypes("mysearch")
                .setQuery(QueryBuilders.fuzzyQuery("say","helOL").fuzziness(Fuzziness.TWO))
                .highlighter(new HighlightBuilder().field("say").preTags("<font style='color:red'>").postTags("</font>"))
                .get();
        print(searchResponse);
    }


    public void updateIndex(){
        Person guansheng = new Person(5, "宋江", 88, 0, "水泊梁山", "17666666666", "wusong@kkb.com","及时雨宋江");
        client.prepareUpdate().setIndex("indexsearch").setType("mysearch").setId("5")
                .setDoc(JSONObject.toJSONString(guansheng), XContentType.JSON)
                .get();
    }

    public void deleteById(){
        DeleteResponse deleteResponse = client.prepareDelete().setIndex("indexsearch").setType("mysearch").setId("9").execute().actionGet();
//        prepareDelete("indexsearch", "mysearch", 8).get();
        System.out.println(deleteResponse.toString());
    }

    public  void  deleteIndex(){
        AcknowledgedResponse indexsearch = client.admin().indices().prepareDelete("indexsearch").execute().actionGet();
    }


    public void deleteByQyery(){
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE
                .newRequestBuilder(client)
                .filter(QueryBuilders.termQuery("id","5"))
                .source("indexsearch")
                .get();

        long deleted = response.getDeleted();
        System.out.println(deleted);
    }





    public void queryByTerm(){
        SearchResponse searchResponse = client
                .prepareSearch("indexsearch")
                .setTypes("mysearch")
                .setQuery(new TermQueryBuilder("say","熟悉"))
                .get();
        print(searchResponse);
    }
    public void scrollPages() throws InterruptedException {
        //获取Client对象,设置索引名称,搜索类型(SearchType.SCAN)[5.4移除，对于java代码，直接返回index顺序，不对结果排序],搜索数量,发送请求
        SearchResponse searchResponse = client
                .prepareSearch("indexsearch")
                .setTypes("mysearch")
                .setSearchType(SearchType.DEFAULT)//执行检索的类别
                .addSort("id",SortOrder.ASC)
                .setSize(2).setScroll(new TimeValue(10000)).get();
        //获取总数量
        long totalCount = searchResponse.getHits().getTotalHits();
        print(searchResponse);
        System.out.println(totalCount);
        int page=(int)totalCount/(2);//计算总页数
        if(totalCount % 2 >= 1){
            page ++;
        }
        System.out.println("总页数： ================="+page+"=============");
        TimeUnit.SECONDS.sleep(1);
        for (int i = 0; i <= page; i++) {
            String id = searchResponse.getScrollId();
            System.out.println(id);
            searchResponse = client
                    .prepareSearchScroll(id)//再次发送请求,并使用上次搜索结果的ScrollId
                    .setScroll(new TimeValue(10000))
                    .get();
            SearchHits hits = searchResponse.getHits();
            for (SearchHit searchHit : hits) {
                System.out.println(searchHit.getSourceAsString()); // 获取字符串格式打印
            }
        }
    }

    private void print(SearchResponse searchResponse){
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit:hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);


//            //打印我们经过高亮显示之后的数据
//            Text[] says = hit.getHighlightFields().get("say").getFragments();
//            for (Text say : says) {
//                System.out.println(say);
//            }

        }
    }

    private void print(GetResponse documentFields){
        String index = documentFields.getIndex();
        String type = documentFields.getType();
        String id = documentFields.getId();
        System.out.println(index);
        System.out.println(type);
        System.out.println(id);
        Map<String, Object> source = documentFields.getSource();
        for (String s : source.keySet()) {
            System.out.println(source.get(s));
        }

    }
    public void close(){
        client.close();
    }

    public static void main(String[] args) throws UnknownHostException, InterruptedException {
        QueryApiTest queryApiTest = new QueryApiTest();
        queryApiTest.init();;
//        queryApiTest.highLight();
//        queryApiTest.scrollPages();
//        queryApiTest.getPageIndex();
//        queryApiTest.queryBool();
//        queryApiTest.queryByWildCard();
//        queryApiTest.queryByFuzzy();
//        queryApiTest.queryByTerm();
//        queryApiTest.queryById();
//        queryApiTest.queryByRange();
//        queryApiTest.queryAll();

//        queryApiTest.updateIndex();
        queryApiTest.deleteById();
        queryApiTest.deleteIndex();
//        queryApiTest.deleteByQyery();

        queryApiTest.close();
    }
}
