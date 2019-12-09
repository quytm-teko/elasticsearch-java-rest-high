package com.teko.tmq.es.controller;

import com.teko.tmq.es.model.SchemaInitialization;
import com.teko.tmq.es.service.ConfigurationService;
import com.teko.tmq.es.util.MemoryStorage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Author: quytm
 * Email : minhquylt95@gmail.com
 * Date  : Dec 08, 2019
 */
@Slf4j
@RestController
@RequestMapping("config")
public class ConfigurationController {

    @Autowired
    private MemoryStorage storage;

    @Autowired
    private ConfigurationService configurationService;

    @PostMapping("/schema")
    public ResponseEntity<String> pushSchema(@RequestBody SchemaInitialization request) {

        log.info("request: indexName = {}", request.getIndexName());

        storage.initSchema(request.getIndexName(), request.getSchema());

        try {
            configurationService.generateSchema(request.getIndexName(), request.getSchema());
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return ResponseEntity.ok("failed");
        }

        return ResponseEntity.ok("done");
    }

    @PostMapping("/{index}/import")
    public ResponseEntity<String> importData(@PathVariable("index") String index, @RequestBody List<Map<String, Object>> data) {
        if (!storage.has(index)) return ResponseEntity.ok("Not found " + index);

        try {
            configurationService.importData(index, data);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return ResponseEntity.ok("failed");
        }

        return ResponseEntity.ok("done");
    }

}
