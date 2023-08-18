package com.miduchina.wrd.elastic.service;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @AUTHOR hanson
 * @SINCE 2023/8/3 11:56
 */
public class DocBaseService {

    private final ElasticsearchRestTemplate elasticsearchRestTemplate;
    private final RestHighLevelClient restHighLevelClient;

    private final Class<?> clazz;

    private String indexName;

    private Field idField;

    Logger log = LoggerFactory.getLogger(DocBaseService.class);

    public DocBaseService(ElasticsearchRestTemplate elasticsearchRestTemplate , RestHighLevelClient restHighLevelClient, Class<?> clazz){
        this.elasticsearchRestTemplate = elasticsearchRestTemplate;
        this.restHighLevelClient=restHighLevelClient;
        this.clazz = clazz;
        init();
    }

    /**
     * 保存
     *
     * @param entity 索引实体
     * @return {@link T}
     */ //todo 这里很遗憾，方法的入参只能限制为实现接口的类型，做不到限制为具体的类型！！！，reason：class无法转为Type
    public<T extends ElasticsearchService> T save (T entity){
        Assert.isTrue(entity.getClass()==clazz,"参数类新不匹配，请传入服务对应的实体类");
        return elasticsearchRestTemplate.save(entity);
    }

    /**
     * 批量保存
     *
     * @param entities 索引实体
     * @return {@link Iterable}<{@link T}>
     */
    public<T extends ElasticsearchService> Iterable<T> batchSave(Iterable<T> entities){
        entities.forEach(x->Assert.isTrue(x.getClass()==clazz,"参数类新不匹配，请传入服务对应的实体类"));
        return elasticsearchRestTemplate.save(entities);
    }

    /**
     * 通过id删除
     *
     * @param docId 索引文档id，需关联实体类id与文档id
     * @return boolean 删除成功返回 true ，id不存在返回true
     */
    public boolean deleteById(String docId){
        Assert.isTrue(StringUtils.hasText(docId),"索引id不能为空");
        String msg = elasticsearchRestTemplate.delete(docId, clazz);
        return docId.equals(msg);
    }

    /**
     * 批量删除
     * 通过ID批量删除
     *
     * @param entities 实体
     * @return long    删除的数量
     */
    public<T> long batchDelete(Iterable<T> entities){
        entities.forEach(x->Assert.isTrue(x.getClass()==clazz,"参数类新不匹配，请传入服务对应的实体类"));
        List<String> ids = new ArrayList<>();
        for (T entity : entities) {

            try {

                String docId = idField.get(entity).toString();
                ids.add(docId);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery().addIds(ids.toArray(new String[0]));
        NativeSearchQuery query = new NativeSearchQuery(idsQueryBuilder);
        ByQueryResponse queryResponse = elasticsearchRestTemplate.delete(query, clazz);
        return queryResponse.getDeleted();
    }

    /**
     * 删除
     *
     * @param entity 实体,更据id删除
     * @return docId
     */
    public<T> String delete(T entity){
        Assert.isTrue(entity.getClass()==clazz,"参数类新不匹配，请传入服务对应的实体类");
        return elasticsearchRestTemplate.delete(entity);
    }

    /**
     * 删除根据查询
     *
     * @param query 查询
     * @return long 删除数
     */
    public long deleteByQuery(Query query){
//        BoolQueryBuilder builder = QueryBuilders.boolQuery()
//                .must(QueryBuilders.termQuery("name", "zhangsan"))
//                .must(QueryBuilders.boolQuery().should(QueryBuilders.termQuery("city", "shanghai")).should(QueryBuilders.termQuery("tel", 1222)));
//        NativeSearchQuery nativeSearchQuery = new NativeSearchQuery(builder);

        return elasticsearchRestTemplate.delete(query, clazz).getDeleted();
    }

    /**
     * 查询
     *
     * @param query 查询
     * @return {@link List}<{@link ?}>
     */
    public List<?> query(Query query){
        SearchHits<?> searchHits = elasticsearchRestTemplate.search(query, clazz);
        return searchHits.getSearchHits().stream().map(SearchHit::getContent).collect(Collectors.toList());
    }

    /**
     * 查询一个
     *
     * @param query 查询
     * @return {@link Object}
     */
    public Object queryOne(Query query){
        SearchHit<?> searchHit = elasticsearchRestTemplate.searchOne(query, clazz);
        return searchHit != null ? searchHit.getContent() : null;
    }

    /**
     * 查询通过id
     *
     * @param id id
     * @return {@link Object}
     */
    public Object queryById(String id){
        return elasticsearchRestTemplate.get(id, clazz);
    }

    /**
     * 更新通过id
     *
     * @param docId     文档id
     * @param updateMap 要更新的k-v
     * @return {@link UpdateResponse.Result}
     */
    public UpdateResponse.Result updateById(String docId, Map<String,Object> updateMap){
        Document document = Document.from(updateMap);
        UpdateQuery updateQuery = UpdateQuery.builder(docId).withDocument(document).withDocAsUpsert(false).build();
        UpdateResponse updateResponse = elasticsearchRestTemplate.update(updateQuery, IndexCoordinates.of(indexName));
        return updateResponse.getResult();
    }

    /**
     * 更新查询
     * 更新通过查询,
     * !!!!!!! 该方法不支持更新子对象
     *
     * @param updateMap    要更新的k-v
     * @param queryBuilder 查询构建器
     * @return long         更新数
     */
    public long updateByQuery(QueryBuilder queryBuilder, Map<String,Object> updateMap){


        StringBuilder builder = new StringBuilder();
        updateMap.forEach((k,v)->{
            String s = "ctx._source."+k+"=" + "params."+k+";";
            builder.append(s);
        });

        Script script = new Script(ScriptType.INLINE,"painless",builder.toString(),updateMap);

        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(indexName);
        updateByQueryRequest.setScript(script);
        updateByQueryRequest.setQuery(queryBuilder);
        try {

            BulkByScrollResponse bulkByScrollResponse = restHighLevelClient.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
            return bulkByScrollResponse.getUpdated();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 更新查询
     * 通过脚本更新
     * ctx._source.要修改的字段=修改的值;
     *
     * @param queryBuilder 查询构建器
     * @param script       脚本 eg: "ctx._source.要修改的字段='修改的值';"
     * @return long     更新数
     */
    public long updateByQuery(QueryBuilder queryBuilder, String script){

        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(indexName);
        updateByQueryRequest.setScript(new Script(script));
        updateByQueryRequest.setQuery(queryBuilder);
        try {

            BulkByScrollResponse bulkByScrollResponse = restHighLevelClient.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
            return bulkByScrollResponse.getUpdated();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 更新
     *
     * @param entity 实体
     * @param upsert 不存在时插入
     * @return {@link UpdateResponse.Result}
     *
     */
    private<T> UpdateResponse.Result update(T entity , boolean upsert){
        Assert.isTrue(entity.getClass()==clazz,"参数类新不匹配，请传入服务对应的实体类");
        String docId = null;
        try {
            docId = idField.get(entity).toString();
        } catch (IllegalAccessException e) {
            throw new RuntimeException("获取索引id字段失败");
        }
        Document document = elasticsearchRestTemplate.getElasticsearchConverter().mapObject(entity);
        UpdateQuery updateQuery = UpdateQuery.builder(docId).withDocument(document).withDocAsUpsert(upsert).build();
        return elasticsearchRestTemplate.update(updateQuery, IndexCoordinates.of(indexName)).getResult();
    }

    /**
     * 保存或更新
     *
     * @param entity 实体
     * @return {@link UpdateResponse.Result}
     *
     */
    public<T> UpdateResponse.Result saveOrUpdate(T entity) {
        return update(entity, true);
    }

    /**
     * 更新
     *
     * @param entity 实体
     * @return {@link UpdateResponse.Result}
     *
     */
    public<T> UpdateResponse.Result update(T entity)  {
        return update(entity, false);
    }

    /**
     * 批量更新
     *
     * @param entities 实体
     *
     */
    public<T> void batchUpdate(Iterable<T> entities) {
        entities.forEach(x->Assert.isTrue(x.getClass()==clazz,"参数类新不匹配，请传入服务对应的实体类"));
        List<UpdateQuery> queryList = new ArrayList<>();
        for (T next : entities) {
            Document document = elasticsearchRestTemplate.getElasticsearchConverter().mapObject(next);
            String docId = null;
            try {
                docId = idField.get(next).toString();
            } catch (IllegalAccessException e) {
                throw new RuntimeException("获取索引id字段失败");
            }
            Assert.isTrue(StringUtils.hasText(docId),idField.getName()+" id为空");
            UpdateQuery updateQuery = UpdateQuery.builder(docId).withDocument(document).withDocAsUpsert(false).build();
            queryList.add(updateQuery);
        }

        elasticsearchRestTemplate.bulkUpdate(queryList,BulkOptions.defaultOptions(),IndexCoordinates.of(indexName));
    }

    /**
     * 保存或更新，根据id判断是否存在，不存在则新增
     *
     * @param entities 实体集合
     *
     */
    public<T> void saveOrUpdate(Iterable<T> entities) {
        entities.forEach(x->Assert.isTrue(x.getClass()==clazz,"参数类新不匹配，请传入服务对应的实体类"));
        List<UpdateQuery> queryList = new ArrayList<>();
        for (T next : entities) {
            Document document = elasticsearchRestTemplate.getElasticsearchConverter().mapObject(next);
            String docId = null;
            try {
                docId = idField.get(next).toString();
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            Assert.isTrue(StringUtils.hasText(docId),idField.getName()+" id为空");
            UpdateQuery updateQuery = UpdateQuery.builder(docId).withDocument(document).withDocAsUpsert(true).build();
            queryList.add(updateQuery);
        }

        elasticsearchRestTemplate.bulkUpdate(queryList,BulkOptions.defaultOptions(),IndexCoordinates.of(indexName));
    }






    private void init(){
        for (Field field : clazz.getDeclaredFields()) {
            if (field.getAnnotation(Id.class) != null) {
                this.idField=field;
                break;
            }
        }

        if (idField!=null)
            idField.setAccessible(true);
        else
            log.error("没有找到主键字段 @Id");
        org.springframework.data.elasticsearch.annotations.Document annotation = clazz.getAnnotation(org.springframework.data.elasticsearch.annotations.Document.class);
        this.indexName = Objects.nonNull(annotation) ? annotation.indexName() : "";

        if (!StringUtils.hasText(indexName))
            log.error("【es服务注册监听器】获取索引名失败，，，可能会导致后续es操作失败 at {}",clazz);

        log.info("es服务初始化完成，索引名：{},主键：{}",indexName,idField==null ? "" : idField.getName());
    }
}
