package com.miduchina.wrd.elastic.listener;

import com.miduchina.wrd.elastic.service.ElasticsearchService;
import com.miduchina.wrd.elastic.service.DocBaseService;
import org.elasticsearch.client.RestHighLevelClient;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;

import java.util.Set;

/**
 * es服务注入侦听器
 *
 * @AUTHOR hanson
 * @SINCE 2023/8/3 10:33
 */

public class EsServiceInjectListener implements ApplicationListener<ContextRefreshedEvent> {

    private static final Logger log = LoggerFactory.getLogger(EsServiceInjectListener.class);

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        log.info("【es服务注册监听器】开始执行");
        ConfigurableApplicationContext applicationContext = (ConfigurableApplicationContext) contextRefreshedEvent.getApplicationContext();
        System.out.println("--------------"+contextRefreshedEvent.getApplicationContext());
        ElasticsearchRestTemplate elasticsearchRestTemplate = applicationContext.getBean(ElasticsearchRestTemplate.class);
        RestHighLevelClient restHighLevelClient = applicationContext.getBean(RestHighLevelClient.class);
        Reflections reflections = new Reflections();
        Set<Class<? extends ElasticsearchService>> subTypesOf = reflections.getSubTypesOf(ElasticsearchService.class);
        subTypesOf.forEach(subClass -> {

            String serviceName = null;
            try {
                serviceName = subClass.newInstance().serviceName();

            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            DocBaseService docBaseService = new DocBaseService(elasticsearchRestTemplate, restHighLevelClient, subClass);
            applicationContext.getBeanFactory().registerSingleton(serviceName, docBaseService);
            log.info("【es服务注册监听器】向容器中注入bean：{}",serviceName);
        });

    }


}
