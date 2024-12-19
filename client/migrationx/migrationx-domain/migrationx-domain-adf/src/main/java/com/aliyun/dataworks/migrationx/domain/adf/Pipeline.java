package com.aliyun.dataworks.migrationx.domain.adf;

import com.google.gson.JsonObject;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class Pipeline {
    private String id;
    private String name;
    private String type;
    private PipelineProperty properties;
    private String etag;

    @Data
    public static class PipelineProperty {
        private List<Activity> activities;
        private JsonObject parameters;
        private List<?> annotations;
        private PipelineFolder folder;
        private String lastPublishTime;

        @Data
        public static class PipelineFolder {
            private String name;
        }
        @Data
        public static class Activity {
            private String name;
            private String state;
            private String description;
            private String type;
            private List<DependActivity> dependsOn;
            private Policy policy;
            private List<JsonObject> userProperties;
            private LinkedServiceName linkedServiceName;
            private TypeProperty typeProperties;

            @Data
            public static class TypeProperty {
                PipelineType pipeline;
                boolean waitOnCompletion;
                String notebookPath;
                String method;
                Object url;
                Map<String,Object> headers;
            }

            @Data
            public static class PipelineType {
                String referenceName;
                String type;
            }
            @Data
            public static class DependActivity {
                private String activity;
                private List<String> dependencyConditions;
            }
            @Data
            public static class Policy {
                /**
                 * 活动可运行的最长时间。默认值为 12 小时，最小值为 10 分钟，允许的最长时间为 7 天。格式为 D.HH:MM:SS
                 */
                private String timeout;
                /**
                 * 最大重试尝试次数
                 */
                private Integer retry;
                /**
                 * 每次重试尝试之间间隔的秒数
                 */
                private Integer retryIntervalInSeconds;
                /**
                 * 选中后，将不会在日志记录中捕获活动输出
                 */
                private Boolean secureOutput;
                /**
                 * 选中后，将不会在日志记录中捕获活动输入
                 */
                private Boolean secureInput;
            }

            @Data
            public static class LinkedServiceName {
                private String referenceName;
                private String type;
            }
        }
    }
}

