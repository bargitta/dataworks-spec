package com.aliyun.dataworks.migrationx.domain.adf;

import lombok.Data;

import java.util.Map;

@Data
public class AdfConf {
    private String locale = "zh_CN";
    private AdfSetting settings;
    public static final AdfConf DEFAULT = new AdfConf();
    @Data
    public static class AdfSetting {
        /**
         * adf does not use cron expression, currently users need to manually set cron expression in config file
         */
        Map<String, String> triggers;
        Map<String, String> nodeTypeMappings;
        String unknownNodeType;
    }
}
