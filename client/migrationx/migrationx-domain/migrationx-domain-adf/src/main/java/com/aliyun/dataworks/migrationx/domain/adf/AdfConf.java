package com.aliyun.dataworks.migrationx.domain.adf;

import lombok.Data;

import java.util.List;
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
        /**
         * local folder of notebook files, it has two properties:removePrefix and addPrefix
         * which help convert path on azure to local.
         * e.g.,azure path: /Repos/NGBI/PRD/DWD/dwd_tf_performance_monthly
         * local path: /Users/abc/repo/PRD/DWD/dwd_tf_performance_monthly
         * then removePrefix:  /Repos/NGBI/ and addPrefix:  /Users/abc/repo/
         */
        Map<String, String> notebookLocalPath;
        List<Variable> globalVariables;

        public static class Variable {
            public String name;
            public String value;
        }
    }
}
