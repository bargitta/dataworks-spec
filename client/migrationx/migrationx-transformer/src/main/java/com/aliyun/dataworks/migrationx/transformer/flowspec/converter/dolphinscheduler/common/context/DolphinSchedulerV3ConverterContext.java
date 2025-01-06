/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.common.context;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.ref.SpecDatasource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.ResourceInfo;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.python.PythonVersion;
import com.aliyun.dataworks.migrationx.transformer.flowspec.model.SpecRefEntityWrapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-06-07
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DolphinSchedulerV3ConverterContext {

    /**
     * Some tasks are converted to multiple nodes, and the head nodes of multiple nodes needs to be recorded to facilitate the establishment of
     * dependency relationships
     */
    private final Map<Long, List<SpecRefEntityWrapper>> entityHeadMap = new HashMap<>();

    /**
     * See {@link DolphinSchedulerV3ConverterContext#entityHeadMap}
     */
    private final Map<Long, List<SpecRefEntityWrapper>> entityTailMap = new HashMap<>();

    /**
     * file name and full name map
     */
    private final Map<String, String> fileNameMap = new HashMap<>();

    /**
     * task code and node id map
     */
    private final Map<Long, String> codeUuidMap = new HashMap<>();

    /**
     * Records the entities that have been converted so far
     */
    private final Map<String, SpecRefEntityWrapper> specRefEntityMap = new HashMap<>();

    /**
     * use to record sub workflows in the current workflow
     */
    private final List<SpecWorkflow> subWorkflows = new ArrayList<>();


    /*
    =========================================================================
    The following configuration should be specified in the configuration file
    =========================================================================
     */

    /**
     * Spec version
     */
    private String specVersion = "1.2.0";

    /**
     * Data source map
     */
    private Map<String, SpecDatasource> dataSourceMap;

    /**
     * Default script path
     */
    private String defaultScriptPath;

    /**
     * Default python version
     */
    private PythonVersion pythonVersion;

    /**
     * Resource list
     */
    private List<ResourceInfo> resourceInfoList;

    /**
     * Dependent specification
     */
    private List<Specification<DataWorksWorkflowSpec>> dependSpecification;

    /**
     * dolphin scheduler code will be converted to uuid directly when it is true.
     * otherwise, it will be converted to uuid randomly
     */
    private Boolean directMappingId = false;

    /**
     * whether to judge the condition only once and immediately exit the node
     */
    private Boolean judgeConditionOnce = false;

    /**
     * Specify the task target conversion type, or use the default conversion target if not specified
     */
    private Map<String, String> nodeTypeMap = new HashMap<>();

    public String getUuidFromCode(Long code) {
        if (code == null) {
            return null;
        }
        return codeUuidMap.getOrDefault(code, String.valueOf(code));
    }

}
