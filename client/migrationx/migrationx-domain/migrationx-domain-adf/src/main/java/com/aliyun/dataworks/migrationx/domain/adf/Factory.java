package com.aliyun.dataworks.migrationx.domain.adf;

import lombok.Data;

@Data
public class Factory {
    private String name;
    private Identity identity;
    private String id;
    private String type;
    private FactoryProperty properties;
    private String eTag;
    private String location;
    private Tags tags;

    @Data
    public static class FactoryProperty {
        private String provisioningState;
        private String createTime;
        private String version;
    }
}
