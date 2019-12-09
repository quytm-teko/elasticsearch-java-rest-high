package com.teko.tmq.es.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;

/**
 * Author: quytm
 * Email : minhquylt95@gmail.com
 * Date  : Dec 08, 2019
 */
@Getter
@Setter
public class SchemaInitialization implements Serializable {

    private String indexName;

    private Map<String, Object> schema;

}
