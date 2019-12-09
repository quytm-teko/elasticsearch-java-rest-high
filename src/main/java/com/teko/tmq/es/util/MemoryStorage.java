package com.teko.tmq.es.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Author: quytm
 * Email : minhquylt95@gmail.com
 * Date  : Dec 08, 2019
 */
@Slf4j
@Component
public class MemoryStorage {

    private HashMap<String, String> storage = new HashMap<>();

    public void initSchema(String indexName, Map schema) {
        try {
            String json = new ObjectMapper().writeValueAsString(schema);
            storage.put(indexName, json);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage(), e);
        }
    }

    public String getSchema(String indexName) {
        return storage.get(indexName);
    }

    public boolean has(String indexName) {
//        return storage.containsKey(indexName);
        return true;
    }

}
