package com.miduchina.wrd.elastic.service;

/**
 * @AUTHOR hanson
 * @SINCE 2023/8/3 11:24
 *
 * 实现该接口的实体类，将被认为是Es的实体类，将创建该实体类专有的Es操作Service
 */
public interface ElasticsearchService {
    /**
     * Es操作服务名称 ， 用于获取bean
     *
     * @return {@link String}
     */
    public String serviceName();

}
