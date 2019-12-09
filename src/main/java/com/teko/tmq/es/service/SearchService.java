package com.teko.tmq.es.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Author: quytm
 * Email : minhquylt95@gmail.com
 * Date  : Dec 09, 2019
 */
@Service
public class SearchService {

    @Autowired
    private RestHighLevelClient client;

    public List<Map> search(String indexName) throws IOException {
        List<Map> data = new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        searchSourceBuilder.size(5);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            Map document = new ObjectMapper().readValue(searchHit.getSourceAsString(), Map.class);
            data.add(document);
        }
        return data;
    }

    public List<Map> search(String indexName, Map<String, String> params) throws IOException {
        Optional<Map.Entry<String, String>> opt = params.entrySet().stream().findFirst();
        if (!opt.isPresent()) return new LinkedList<>();

        String property = opt.get().getKey();
        String value = opt.get().getValue();

        List<Map> users = new ArrayList<>();
        QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(property, value);
//                .fuzziness(Fuzziness.AUTO)
//                .prefixLength(2)
//                .maxExpansions(10);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(matchQueryBuilder);
        sourceBuilder.from(0);
        sourceBuilder.size(5);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            Map document = new ObjectMapper().readValue(searchHit.getSourceAsString(), Map.class);
            users.add(document);
        }
        return users;
    }

}
