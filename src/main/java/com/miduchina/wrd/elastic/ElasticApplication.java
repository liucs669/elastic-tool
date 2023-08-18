package com.miduchina.wrd.elastic;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @AUTHOR hanson
 * @SINCE 2023/7/27 17:35
 */
@SpringBootApplication
public class ElasticApplication {
    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(ElasticApplication.class, args);

    }
}
