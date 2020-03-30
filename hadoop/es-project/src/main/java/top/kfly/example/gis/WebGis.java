package top.kfly.example.gis;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class WebGis {

    private static TransportClient client;

    @Test
    public void queryByGis(){
        // 1. 找出落在指定矩形框当中的坐标点
        SearchResponse searchResponse = client
                .prepareSearch("platform_foreign_website")
                .setTypes("store")
                .setQuery(QueryBuilders
                        .geoBoundingBoxQuery("location")
                        .setCorners(40.0519526142, 116.4178513254, 40.0385828363, 116.4465266673)
                ).get();
        for(SearchHit searchHit : searchResponse.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }



        /**|
         * 找出坐落在多边形当中的坐标点
         *
         * 40.0519526142,116.4178513254
         *
         *          40.0519526142,116.4178513254
         *
         *          40.0385828363,116.4465266673
         *
         *
         */
        List<GeoPoint> points = new ArrayList<GeoPoint>();
        points.add(new GeoPoint(40.0519526142, 116.4178513254));
        points.add(new GeoPoint(40.0519526142, 116.4178513254));
        points.add(new GeoPoint(40.0385828363, 116.4465266673));
        searchResponse = client.prepareSearch("platform_foreign_website")
                .setTypes("store")
                .setQuery(QueryBuilders.geoPolygonQuery("location", points))
                .get();

        for(SearchHit searchHit : searchResponse.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }
        System.out.println("==================================================");



        /**
         * 以当前的点为中心，搜索落在半径范围内200公里的经纬度坐标点
         *40.0488115498,116.4320345091
         */
        searchResponse = client.prepareSearch("platform_foreign_website")
                .setTypes("store")
                .setQuery(QueryBuilders.geoDistanceQuery("location")
                        .point(40.0488115498, 116.4320345091)
                        .distance(200, DistanceUnit.KILOMETERS))
                .get();

        for(SearchHit searchHit : searchResponse.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }

    }
    @Before
    public void init() throws UnknownHostException {
        Settings myes = Settings.builder()
                .put("cluster.name", "myes")
                .put("client.transport.sniff", "false")
                .build();

        TransportAddress address = new TransportAddress(InetAddress.getByName("node01"),9300);

        client = new PreBuiltTransportClient(myes).addTransportAddress(address);
    }

    @After
    public void close(){
        client.close();
    }


}
