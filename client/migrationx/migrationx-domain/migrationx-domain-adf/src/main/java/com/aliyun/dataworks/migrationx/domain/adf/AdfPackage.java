package com.aliyun.dataworks.migrationx.domain.adf;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Data
public class AdfPackage extends com.aliyun.dataworks.migrationx.domain.dataworks.standard.objects.Package {
    private String factory;

    /**
     * mapping from pipeline name to its trigger
     */
    private Map<String, Trigger> triggers;

    private List<Pipeline> pipelines;

    private List<LinkedService> linkedServices;
}
