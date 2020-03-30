package top.kfly.example.project;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class BusinessCore {

    private static TransportClient client;

    @Before
    public void init() throws UnknownHostException {
        Settings myes = Settings.builder()
                .put("cluster.name", "myes")
                .put("client.transport.sniff", "false")
                .build();

        TransportAddress address = new TransportAddress(InetAddress.getByName("node01"),9300);

        client = new PreBuiltTransportClient(myes).addTransportAddress(address);
    }

    /**
     * 需求一：统计每个球队中球员的个数
     * select team , count(1) count from player group by team;
     */
//    @Test
    public void countPlayerGroupByTeam(){
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("player").setTypes("player");
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("player count").field("team");
        searchRequestBuilder.addAggregation(termsAggregationBuilder);
        Aggregations aggregations = searchRequestBuilder.get().getAggregations();
        aggregations.forEach(action->{
            StringTerms stringTerms = (StringTerms) action;
            List<StringTerms.Bucket> buckets = stringTerms.getBuckets();
            buckets.forEach(bucket -> {
                System.out.println(bucket.getKey());
                System.out.println(bucket.getDocCount());
                System.out.println(bucket.getKeyAsString());
            });
        });
    }


    /**
     * 需求二：统计每个球队中，不同位置的球员
     * select team ,position, count(1) count from player group by team;
     */
//    @Test
    public void countPlayeAndPositionrGroupByTeam(){
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("player").setTypes("player");
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("player count").field("team");
        TermsAggregationBuilder positionAggregation = AggregationBuilders.terms("position_count").field("position");
        // 二级聚合
        termsAggregationBuilder.subAggregation(positionAggregation);

        SearchResponse searchResponse = searchRequestBuilder.addAggregation(termsAggregationBuilder).get();

        Aggregations aggregations = searchResponse.getAggregations();
        aggregations.forEach(action->{
            StringTerms stringTerms = (StringTerms) action;
            List<StringTerms.Bucket> buckets = stringTerms.getBuckets();
            buckets.forEach(bucket -> {
                System.out.println("球队：" + bucket.getKey()  + "：" + bucket.getDocCount());
                Aggregation position_count = bucket.getAggregations().get("position_count");
                if(null != position_count){
                    StringTerms stringTerms1 = (StringTerms) position_count;
                    stringTerms1.getBuckets().forEach(bucket1 -> {
                        System.out.println("        位置：" + bucket1.getKey()  + "：" + bucket1.getDocCount());
                    });
                }

            });
        });
    }


    /**
     * 分组求每个球队中球员的最大值
     * select team,max(age) from player group by team
     */
    @Test
    public void maxAgeByGroupByTeam(){
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("player").setTypes("player");

        TermsAggregationBuilder termsAggregationBuilder =
                AggregationBuilders
                        .terms("player count").field("team")
                        .order(BucketOrder.aggregation("sumSalary",false));

        MaxAggregationBuilder maxAge = AggregationBuilders.max("maxAge").field("age");
        MinAggregationBuilder minAge = AggregationBuilders.min("minAge").field("age");
        AvgAggregationBuilder avgAge = AggregationBuilders.avg("avgAge").field("age");
        SumAggregationBuilder sumSalary = AggregationBuilders.sum("sumSalary").field("salary");
        termsAggregationBuilder.subAggregation(maxAge);
        termsAggregationBuilder.subAggregation(minAge);
        termsAggregationBuilder.subAggregation(avgAge);
        termsAggregationBuilder.subAggregation(sumSalary);

        SearchResponse searchResponse = searchRequestBuilder
                .addAggregation(termsAggregationBuilder)
                .get();
        Aggregations aggregations = searchResponse.getAggregations();
        aggregations.forEach(aggregation -> {
            StringTerms stringTerms = (StringTerms)aggregation;
            List<StringTerms.Bucket> buckets = stringTerms.getBuckets();
            buckets.forEach(bucket -> {
                Object key = bucket.getKey();
                Aggregation maxAge1 = bucket.getAggregations().get("maxAge");
                Aggregation minAge1 = bucket.getAggregations().get("minAge");
                Aggregation avgAge1 = bucket.getAggregations().get("avgAge");
                Aggregation sumSalary1 = bucket.getAggregations().get("sumSalary");
                System.out.println(key + ":" + bucket.getDocCount() + ":" + maxAge1 +":" + minAge1 + ":" + avgAge1 + ":" + sumSalary1);
            });
        });
    }
    @After
    public void close(){
        client.close();
    }

}
