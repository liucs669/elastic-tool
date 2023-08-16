elasticsearch 工具

版本说明：springboot 2.6.14
        spring-data-elasticsearch 4.3.0
        elaticsearch 7.15
        jdk 8

工具分为两部分：1，对索引的操作 IndexBaseService 类
             2，对索引下文档的操作 DocBaseService 类


使用说明：
    1，操作索引
    @AutoWried
    IndexBaseService indexBaseService；

    创建删除索引，获取索引的设置，映射信息，判断索引是否存在
    api： (详情见IndexBaseService源码说明)

           createIndex(Class<?> clazz)                     创建索引，通过类上的注解信息获取索引信息，@Document ， @Mapping ，@Setting ， @Field
           createIndex(Class<?> clazz, Document mapping)   创建索引，通过类上的注解信息获取索引信息，@Document ，指定mapping
           createIndex(Class<?> clazz,Document mapping,Settings settings)      创建索引，通过类上的注解信息获取索引信息，@Document ，指定mapping ， setting
           createIndex(String indexName,String alias)           创建索引,索引与别名不能相同
           createIndex(String indexName,String alias, Document mapping )       创建索引
           createIndex(String indexName,String alias, Document mapping , Settings settings)        创建索引
           createDefaultSettings       默认设置 一分区 一副本
           createSettings(Class<?> clazz)          通过类上{@Settings}注解，获取索引设置信息
           createMapping(Class<?> clazz)       通过类的注解获取mapping信息
           getIndexSettings(Class<?> clazz)        查询索引设置
           getIndexSettings(String indexName)      查询索引设置
           getIndexMapping(Class<?> clazz)         得到索引mapping
           getIndexMapping(String indexName)         得到索引mapping
           deleteIndex(Class<?> clazz)             删除索引
           deleteIndex(String indexName)           删除索引
           existIndex(Class<?> clazz)              索引是否存在
           existIndex(String indexName)            索引是否存在

    2，索引下文档操作

        1），es实体类实现 com.midu.elastic.service.ElasticsearchService 接口。
            实现接口serviceName方法，返回该实体类对应的服务的名称，名称需要全局唯一，非空。

        2），自动注入 获取服务类
            @Resource（serviceName）
            Service service

        3），使用service，进行crud
            详情见DocBaseService类源码

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

                    org.springframework.data.elasticsearch.annotations.Document annotation = clazz.getAnnotation(org.springframework.data.elasticsearch.annotations.Document.class);
                    this.indexName = Objects.nonNull(annotation) ? annotation.indexName() : "";

                    if (!StringUtils.hasText(indexName))
                        log.error("【es服务注册监听器】获取索引名失败，，，可能会导致后续es操作失败 at {}",clazz);

                    log.info("es服务初始化完成，索引名：{},主键：{}",indexName,idField.getName());
                }
            }




