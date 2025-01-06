package com.aliyun.dataworks.migrationx.domain.adf;

import lombok.Data;
@Data
public class LinkedService {
    private String id;
    private String name;
    private String type;
    private LinkedServiceProperty properties;
    private String etag;

    @Data
    public static class LinkedServiceProperty {
        private String type;
        private TypeProperty typeProperties;
        private String description;

        @Data
        public static class TypeProperty {
            private String connectionString;
            private String encryptedCredential;
        }
    }
}