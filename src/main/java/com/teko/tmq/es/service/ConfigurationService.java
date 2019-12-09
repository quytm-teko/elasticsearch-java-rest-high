package com.teko.tmq.es.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: quytm
 * Email : minhquylt95@gmail.com
 * Date  : Dec 08, 2019
 */
@Slf4j
@Service
public class ConfigurationService {

    @Autowired
    private RestHighLevelClient client;

    public void generateSchema(String indexName, Map<String, Object> schema) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(indexName);

        Map<String, Object> message = new HashMap<>();
        message.put("type", "text");

        Map<String, Object> properties = new HashMap<>();
        schema.forEach((property, dataType) -> {
            String esType = convertToESType(String.valueOf(dataType));
            if (StringUtils.isEmpty(esType)) return;

            properties.put(property, message);
        });

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);
        request.mapping(mapping);

        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        if (!exists) {
            CreateIndexResponse indexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            log.info("Response id: " + indexResponse.index());
        }
    }

    public void importData(String indexName, List<Map<String, Object>> data) throws IOException {
        for (Map<String, Object> document : data) {
            if (!isValidDataSchema()) continue;

            IndexRequest request = new IndexRequest(indexName);
            request.id(System.currentTimeMillis() + "");
            String requestStr = new ObjectMapper().writeValueAsString(document);
            request.source(requestStr, XContentType.JSON);
            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
            // client.indexAsync(request, RequestOptions.DEFAULT, listener); // async
            log.info("response id: {}", indexResponse.getId());
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    private String convertToESType(String type) {
        switch (type) {
            case "string":
                return "text";
            default:
                return "";
        }
    }

    private boolean isValidDataSchema() {
        // use MemoryStorage to check
        return true;
    }

    // -----------------------------------------------------------------------------------------------------------------

    private ActionListener listener = new ActionListener<IndexResponse>() {
        @Override
        public void onResponse(IndexResponse indexResponse) {
            log.info(" Document updated successfully !!!");
        }

        @Override
        public void onFailure(Exception e) {
            log.error(" Document creation failed !!! " + e.getMessage(), e);
        }
    };

}
