package com.midu.elastic.config;

/**
 * @AUTHOR hanson
 * @SINCE 2023/7/28 11:26
 */
import com.midu.elastic.prop.EsBaseProperties;
import com.midu.elastic.service.IndexBaseService;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.data.elasticsearch.config.AbstractElasticsearchConfiguration;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


@Configuration
@EnableConfigurationProperties(EsBaseProperties.class)
public class ElasticToolAutoConfiguration extends AbstractElasticsearchConfiguration {

//    @Value("${spring.elasticsearch.rest.uris}")
//    private List<String> uris;
//    @Value("${spring.elasticsearch.rest.username:}")
//    private String userName;
//    @Value("${spring.elasticsearch.rest.password:}")
//    private String password;

    private final EsBaseProperties esBaseProperties;


    Logger logger = LoggerFactory.getLogger(ElasticToolAutoConfiguration.class);

    public ElasticToolAutoConfiguration(EsBaseProperties esBaseProperties) {
        this.esBaseProperties = esBaseProperties;
    }

    @Override
    @Bean
//    @ConditionalOnProperty(prefix = "elastic.tool",name = "uris")
    public RestHighLevelClient elasticsearchClient() {

        ClientConfiguration.ClientConfigurationBuilderWithRequiredEndpoint builder = ClientConfiguration.builder();

        ClientConfiguration.MaybeSecureClientConfigurationBuilder maybeSecureClientConfigurationBuilder = builder.connectedTo(esBaseProperties.getUris().toArray(new String[0]));

        if (StringUtils.hasText(esBaseProperties.getUsername()) && StringUtils.hasText(esBaseProperties.getPassword())){
            maybeSecureClientConfigurationBuilder.withBasicAuth(esBaseProperties.getUsername(),esBaseProperties.getPassword());
        }

//        maybeSecureClientConfigurationBuilder.usingSsl();

        maybeSecureClientConfigurationBuilder.withConnectTimeout(Duration.ofSeconds(5))
                .withSocketTimeout(Duration.ofSeconds(3));

        maybeSecureClientConfigurationBuilder.withHeaders(()->{
            HttpHeaders httpHeaders = new HttpHeaders();
            httpHeaders.add("currentTime", LocalDateTime.now().format(DateTimeFormatter.BASIC_ISO_DATE));
            return httpHeaders;
        });

        ClientConfiguration clientConfiguration = maybeSecureClientConfigurationBuilder.build();

        logger.info("【ElasticTool】 client 初始化完成。。。。。。。。。。。。。");
        return RestClients.create(clientConfiguration).rest();
    }


    @Bean
//    @ConditionalOnBean(RestHighLevelClient.class)
    public ElasticsearchRestTemplate elasticsearchRestTemplate(RestHighLevelClient restHighLevelClient){
        logger.info("【ElasticTool】 template 初始化完成。。。。。。。。。。。。。");
        return new ElasticsearchRestTemplate(restHighLevelClient);
    }

    @Bean
//    @ConditionalOnBean(ElasticsearchRestTemplate.class)
    public IndexBaseService indexBaseService(ElasticsearchRestTemplate elasticsearchRestTemplate ,RestHighLevelClient restHighLevelClient){
        return new IndexBaseService(elasticsearchRestTemplate,restHighLevelClient);
    }

}






