package com.teko.tmq.es.controller;

import com.teko.tmq.es.service.SearchService;
import com.teko.tmq.es.util.MemoryStorage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.util.ObjectUtils;
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
 * Date  : Dec 09, 2019
 */
@Slf4j
@RestController
@RequestMapping("/search")
public class SearchController {

    @Autowired
    private MemoryStorage storage;

    @Autowired
    private SearchService searchService;

    @PostMapping("/{index}/params")
    public ResponseEntity searchByParams(@PathVariable("index") String index,
                                         @RequestBody(required = false) Map<String, String> params) {
        if (!storage.has(index)) return ResponseEntity.ok("Not found " + index);

        try {
            List<Map> data;
            if (ObjectUtils.isEmpty(params)) {
                data = searchService.search(index);
            } else {
                data = searchService.search(index, params);
            }
            return ResponseEntity.ok(data);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return ResponseEntity.ok("Failed");
        }

    }

}
