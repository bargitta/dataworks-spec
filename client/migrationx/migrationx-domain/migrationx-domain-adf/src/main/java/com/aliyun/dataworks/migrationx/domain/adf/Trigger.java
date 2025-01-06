package com.aliyun.dataworks.migrationx.domain.adf;

import lombok.Data;

import java.util.List;

@Data
public class Trigger {
    private String name;
    private String id;

    private String type; //constant value: "Microsoft.DataFactory/factories/triggers"
    private TriggerProperty properties;

    @Data
    public static class TriggerTypeProperty {
        private Recurrence recurrence;
    }

    @Data
    public static class Recurrence {
        String frequency;
        int interval;
        String startTime;
        String timeZone;
        Schedule schedule;
    }
    @Data
    public static class Schedule {
        List<Integer> minutes;
        List<Integer> hours;
    }
    @Data
    public static class TriggerProperty {
        String description;
        String runtimeState; // "Started" means running trigger
        String type; //constant value "ScheduleTrigger"
        List<LinkedPipeline> pipelines;
        TriggerTypeProperty typeProperties;
    }

    @Data
    public static class LinkedPipeline {
        PipelineReference pipelineReference;
    }

    @Data
    public static class PipelineReference {
        /**
         * pipeline name
         */
        String referenceName;
        /**
         * PipelineReference
         */
        String type;
    }
}
