package com.example.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.example.elasticsearch.config.ElasticsearchConfig;
import lombok.Data;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Random;

@SpringBootTest
class ElasticsearchDemoApplicationTests {

    @Autowired
    private RestHighLevelClient client;

    @Test
    void contextLoads() {
        System.out.println(client);
    }

    @Test
    void indexData() throws IOException {
        Random randomAge = new Random();
        for (int i = 0; i < 10000; i++) {
            User user = new User();
            user.setUsername("zhang san" + randomAge.nextInt(100));
            user.setAge(randomAge.nextInt(100));
            user.setGender(randomAge.nextBoolean()?"男":"女");
            user.setCreateTime(LocalDateTime.now());
            String jsonString = JSON.toJSONString(user);
            IndexRequest indexRequest = new IndexRequest("user")
                    .id(String.valueOf(i))
                    .source(jsonString, XContentType.JSON);
            IndexResponse index = client.index(indexRequest, ElasticsearchConfig.REQUEST_OPTIONS);
            System.out.println(index);
        }
    }

    @Test
    void searchData() throws Exception {
        // 1、创建检索请求
        SearchRequest searchRequest = new SearchRequest();
        // 指定索引
        searchRequest.indices("bank");
        // 指定DSL，检索条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 1.1）、构造检索条件
        //        sourceBuilder.query(QueryBuilders.termQuery("user", "kimchy"));
        //        sourceBuilder.from(0);
        //        sourceBuilder.size(5);
        //        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchSourceBuilder.query(QueryBuilders.matchQuery("address", "mill"));

        // 聚合数据
        // 1.2）、根据年龄分布聚合
        TermsAggregationBuilder ageAgg = AggregationBuilders.terms("ageAgg").field("age").size(10);
        searchSourceBuilder.aggregation(ageAgg);

        // 1.3）、计算平薪资
        AvgAggregationBuilder balanceAvg = AggregationBuilders.avg("balanceAvg").field("balance");
        searchSourceBuilder.aggregation(balanceAvg);

        System.out.println("检索条件：" + searchSourceBuilder.toString());
        searchRequest.source(searchSourceBuilder);

        // 2、执行检索
        SearchResponse searchResponse = client.search(searchRequest, ElasticsearchConfig.REQUEST_OPTIONS);

        // 3、分析结果 searchResponse
        System.out.println(searchResponse.toString());

        //3.1）、获取所有查到的数据
        SearchHits hits = searchResponse.getHits(); // 获取到最外围的 hits
        SearchHit[] searchHits = hits.getHits(); // 内围的 hits 数组
        for (SearchHit hit : searchHits) {
            /**
             * "_index":"bank",
             *        "_type":"account",
             *        "_id":"970",
             *        "_score":5.4032025,
             *        "_source":{
             */
            //            hit.getIndex();hit.getType()''
            String str = hit.getSourceAsString();
//            Account account = JSON.parseObject(str, Account.class);
//
//            System.out.println(account.toString());
            System.out.println(str);

        }
        //3.1）、获取这次检索到的分析数据
        Aggregations aggregations = searchResponse.getAggregations();
        // 可以遍历获取聚合数据
        //        for (Aggregation aggregation : aggregations.asList()) {
        //            System.out.println("当前聚合："+aggregation.getName());
        //            aggregation.getXxx
        //        }
        // 也可使使用下面的方式
        Terms ageAgg1 = aggregations.get("ageAgg");
        for (Terms.Bucket bucket : ageAgg1.getBuckets()) {
            String keyAsString = bucket.getKeyAsString();
            System.out.println("年龄：" + keyAsString + " ==> 有 " + bucket.getDocCount() + " 个");
        }

        Avg balanceAvg1 = aggregations.get("balanceAvg");
        System.out.println("平均薪资：" + balanceAvg);
    }

    @Data
    static class User {
        private String username;
        private Integer age;
        private String gender;
        private LocalDateTime createTime;
    }


}
