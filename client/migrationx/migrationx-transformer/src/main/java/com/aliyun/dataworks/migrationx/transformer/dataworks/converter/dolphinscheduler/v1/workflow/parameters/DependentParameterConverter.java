/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v1.workflow.parameters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.DolphinSchedulerV1Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.DependentTaskModel;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.ProcessMeta;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.TaskNode;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.task.dependent.DependentParameters;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class DependentParameterConverter extends AbstractParameterConverter<DependentParameters> {

    public DependentParameterConverter(Properties properties, SpecWorkflow specWorkflow, ProcessMeta processMeta, TaskNode taskDefinition) {
        super(properties, specWorkflow, processMeta, taskDefinition);
    }

    @Override
    protected void convertParameter(SpecNode specNode) {
        List<SpecVariable> specVariableList = convertSpecNodeParam(specNode);

        convertFileResourceList(specNode);
        CodeProgramType codeProgramType = CodeProgramType.VIRTUAL;

        SpecScript script = new SpecScript();
        String language = codeToLanguageIdentifier(codeProgramType);
        script.setLanguage(language);
        //runtime
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setEngine(codeProgramType.getCalcEngineType().getLabel());
        runtime.setCommand(codeProgramType.getName());
        script.setRuntime(runtime);

        script.setPath(getScriptPath(specNode));
        script.setParameters(ListUtils.emptyIfNull(specVariableList).stream().filter(v -> !VariableType.NODE_OUTPUT.equals(v.getType()))
                .collect(Collectors.toList()));
        specNode.setScript(script);
        DolphinSchedulerV1Context context = DolphinSchedulerV1Context.getContext();
        List<String> deps = convertDeps();
        context.getSpecNodeProcessCodeMap().put(specNode, deps);
    }

    public List<String> convertDeps() {
        log.info("params : {}", taskDefinition.getParams());
        DependentParameters dependentParameters = taskDefinition.getDependence();

        if (dependentParameters == null || dependentParameters.getDependTaskList() == null || dependentParameters.getDependTaskList().isEmpty()) {
            log.warn("no dependence param {}", taskDefinition.getParams());
            return Collections.emptyList();
        }
        DolphinSchedulerV1Context context = DolphinSchedulerV1Context.getContext();

        // 本节点的条件依赖
        List<DependentTaskModel> dependencies = dependentParameters.getDependTaskList();
        List<String> taskIds = new ArrayList<>();
        ListUtils.emptyIfNull(dependencies).forEach(dependModel ->
                ListUtils.emptyIfNull(dependModel.getDependItemList()).forEach(depItem -> {
                    String tasks = depItem.getDepTasks();
                    if (StringUtils.equalsIgnoreCase("all", tasks)) {
                        //all leaf task of process definition
                        //1. get all relation of process
                        // preTaskCode -> postTaskCode
                        //Map<Long, List<Long>> relations = findRelations(depItem);
                        List<Object> taskDefinitions = context.getProcessCodeTaskRelationMap().get(depItem.getDefinitionId());
                        for (Object task : CollectionUtils.emptyIfNull(taskDefinitions)) {
                            TaskNode taskNode = (TaskNode) task;
                            //2. find all taskCode not in preTaskCode (not as a pre dependent, leaf task)
                            if (!taskIds.contains(taskNode.getId())) {
                                taskIds.add(taskNode.getId());
                            }
                        }
                    } else {
                        if (!taskIds.contains(depItem.getDepTasks())) {
                            taskIds.add(depItem.getDepTasks());
                        }
                    }
                }));
        return taskIds;
    }
}
