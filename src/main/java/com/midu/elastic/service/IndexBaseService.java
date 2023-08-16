package com.midu.elastic.service;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.index.Settings;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * @AUTHOR hanson
 * @SINCE 2023/8/2 09:56
 */

public class IndexBaseService {

    private final ElasticsearchRestTemplate elasticsearchRestTemplate;
    private final RestHighLevelClient restHighLevelClient;

    public IndexBaseService(ElasticsearchRestTemplate elasticsearchRestTemplate , RestHighLevelClient restHighLevelClient){
        this.elasticsearchRestTemplate = elasticsearchRestTemplate;
        this.restHighLevelClient=restHighLevelClient;
    }

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchRestTemplate.class);

    /**
     * 分区数量
     */
    public static final String NUMBER_OF_SHARDS = "number_of_shards";
    /**
     * 副本数量
     */
    public static final String NUMBER_OF_REPLICAS = "number_of_replicas";


    //    ======================================INDEX OPERATION=================================================

    /**
     * 创建索引，通过类上的注解信息获取索引信息，@Document ， @Mapping ，@Setting ， @Field
     *
     * @param clazz es实体类
     * @return
     */
    public boolean createIndex(Class<?> clazz){
        Document mapping = createMapping(clazz);
        Settings settings = createSettings(clazz);
        return createIndex(clazz,mapping,settings);
    }

    /**
     * 创建索引
     * 创建索引，通过类上的注解信息获取索引信息，@Document ，指定mapping
     *
     * @param clazz   es实体类
     * @param mapping 映射
     * @return boolean
     */
    public boolean createIndex(Class<?> clazz, Document mapping){
        Settings settings = createSettings(clazz);
        return createIndex(clazz,mapping,settings);
    }

    /**
     * 创建索引
     * 创建索引，通过类上的注解信息获取索引信息，@Document ，指定mapping ， setting
     *
     * @param clazz    es实体类
     * @param mapping  映射
     * @param settings 设置
     * @return boolean
     */
    public boolean createIndex(Class<?> clazz,Document mapping,Settings settings){
        return elasticsearchRestTemplate.indexOps(clazz).create(settings, mapping);
    }

    /**
     * 创建索引,索引与别名不能相同
     *
     * @param indexName 索引名称
     * @param alias     别名
     * @return boolean
     */
    public boolean createIndex(String indexName,String alias){
       return createIndex(indexName,alias,null);
    }

    /**
     * 创建索引
     *
     * @param indexName 索引名称
     * @param mapping   映射
     * @param alias     别名
     * @return boolean
     */
    public boolean createIndex(String indexName,String alias, Document mapping ){
       return createIndex(indexName,alias,mapping, createDefaultSettings());
    }

    /**
     * 创建索引
     *
     * @param indexName 索引名称
     * @param mapping   映射
     * @param settings  设置
     * @param alias     别名
     * @return boolean
     */
    public boolean createIndex(String indexName,String alias, Document mapping , Settings settings){
        Assert.isTrue(StringUtils.hasText(indexName),"索引名不能为空");
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        if (Objects.nonNull(settings))
            request.settings(settings);
        if (Objects.nonNull(mapping))
            request.mapping(mapping);
        if (StringUtils.hasText(alias))
            request.alias(new Alias(alias));

        try {
            CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
            return response.isAcknowledged();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取默认设置 , 1分区 1副本
     *
     * @return {@link Settings}
     */
    public Settings createDefaultSettings(){
        Settings settings = new Settings();
        settings.put(NUMBER_OF_SHARDS,1);
        settings.put(NUMBER_OF_REPLICAS,1);
        return settings;
    }

    /**
     * 通过类上{@Settings}注解，获取索引设置信息
     *
     * @param clazz clazz
     * @return {@link Settings}
     */
    public Settings createSettings(Class<?> clazz){
        return elasticsearchRestTemplate.indexOps(clazz).createSettings();
    }

    /**
     * 通过类的注解获取mapping信息
     *
     * @param clazz clazz
     * @return {@link Document}
     */
    public Document createMapping(Class<?> clazz){
        return elasticsearchRestTemplate.indexOps(clazz).createMapping(clazz);
    }

    /**
     * 得到索引设置
     *
     * @param clazz 索引实体类
     * @return {@link Settings}
     */
    public Settings getIndexSettings(Class<?> clazz){
        return elasticsearchRestTemplate.indexOps(clazz).getSettings();
    }

    /**
     * 得到索引设置
     *
     * @param indexName 索引名称
     * @return {@link org.elasticsearch.common.settings.Settings}
     */
    public org.elasticsearch.common.settings.Settings getIndexSettings(String indexName){
        GetIndexRequest request = new GetIndexRequest(indexName);
        try {
            GetIndexResponse getIndexResponse = restHighLevelClient.indices().get(request, RequestOptions.DEFAULT);
            return getIndexResponse.getSettings().get(indexName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 得到索引mapping
     *
     * @param clazz 索引实体类
     * @return {@link Settings}
     */
    public Map<String, Object> getIndexMapping(Class<?> clazz){
        return elasticsearchRestTemplate.indexOps(clazz).getMapping();
    }

    /**
     * 得到索引mapping
     *
     * @param indexName 索引名称
     * @return {@link org.elasticsearch.common.settings.Settings}
     */
    public Map<String, Object> getIndexMapping(String indexName){
        GetIndexRequest request = new GetIndexRequest(indexName);
        try {
            GetIndexResponse getIndexResponse = restHighLevelClient.indices().get(request, RequestOptions.DEFAULT);
            return getIndexResponse.getMappings().get(indexName).getSourceAsMap();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 删除索引
     *
     * @param clazz 索引实体类
     * @return boolean
     */
    public boolean deleteIndex(Class<?> clazz){
        return elasticsearchRestTemplate.indexOps(clazz).delete();
    }

    /**
     * 删除索引
     *
     * @param indexName 索引名称
     * @return boolean
     */
    public boolean deleteIndex(String indexName){
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
        try {
            AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            return acknowledgedResponse.isAcknowledged();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 存在索引
     *
     * @param clazz 索引实体类
     * @return boolean
     */
    public boolean existIndex(Class<?> clazz){
        return elasticsearchRestTemplate.indexOps(clazz).exists();
    }

    /**
     * 存在索引
     *
     * @param indexName 索引名称
     * @return boolean
     */
    public boolean existIndex(String indexName){
        GetIndexRequest request = new GetIndexRequest(indexName);
        try {
            return restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }




}
