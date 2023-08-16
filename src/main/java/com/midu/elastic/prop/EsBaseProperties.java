package com.midu.elastic.prop;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * @AUTHOR hanson
 * @SINCE 2023/8/15 14:47
 */
@ConfigurationProperties(prefix = "elastic.tool")
public class EsBaseProperties {

    private List<String> uris;

    private String username;

    private String password;

    public List<String> getUris() {
        return uris;
    }

    public void setUris(List<String> uris) {
        this.uris = uris;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
