/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DolphinSchedulerV3Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.DependentItem;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.DependentTaskModel;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.dependent.DependentParameters;
import com.aliyun.migrationx.common.utils.GsonUtils;

import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;

@Slf4j
public class DependentParameterConverter extends AbstractParameterConverter<DependentParameters> {

    public DependentParameterConverter(Properties properties, SpecWorkflow specWorkflow, DagData processMeta, TaskDefinition taskDefinition) {
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
        DolphinSchedulerV3Context context = DolphinSchedulerV3Context.getContext();
        List<Long> deps = convertDeps();
        context.getSpecNodeProcessCodeMap().put(specNode, deps);
    }

    public List<Long> convertDeps() {
        log.info("params : {}", taskDefinition.getTaskParams());
        JsonObject param = GsonUtils.fromJsonString(taskDefinition.getTaskParams(), JsonObject.class);
        DependentParameters dependentParameters = null;
        if (param.get("dependence") != null) {
            dependentParameters = GsonUtils.fromJson(param.getAsJsonObject("dependence"), DependentParameters.class);
        }
        if (dependentParameters == null || dependentParameters.getDependTaskList() == null || dependentParameters.getDependTaskList().isEmpty()) {
            log.warn("no dependence param {}", taskDefinition.getTaskParams());
            return Collections.emptyList();
        }
        DolphinSchedulerV3Context context = DolphinSchedulerV3Context.getContext();

        // 本节点的条件依赖
        List<DependentTaskModel> dependencies = dependentParameters.getDependTaskList();
        List<Long> codes = new ArrayList<>();
        ListUtils.emptyIfNull(dependencies).forEach(dependModel ->
                ListUtils.emptyIfNull(dependModel.getDependItemList()).forEach(depItem -> {
                    long depCode = depItem.getDepTaskCode();
                    if (depCode == 0L) {
                        //all leaf task of process definition
                        //1. get all relation of process
                        // preTaskCode -> postTaskCode
                        //Map<Long, List<Long>> relations = findRelations(depItem);

                        List<TaskDefinition> taskDefinitions = context.getProcessCodeTaskRelationMap().get(depItem.getDefinitionCode());
                        for (TaskDefinition task : CollectionUtils.emptyIfNull(taskDefinitions)) {
                            //2. find all taskCode not in preTaskCode (not as a pre dependent, leaf task)
                            if (!codes.contains(task.getCode())) {
                                codes.add(task.getCode());
                            }
                        }
                    } else {
                        if (!codes.contains(depCode)) {
                            codes.add(depCode);
                        }
                    }
                }));
        return codes;
    }

    private Map<Long, List<Long>> findRelations(DependentItem depItem) {
        Map<Long, List<Long>> relations = new HashMap<>();
        DolphinSchedulerV3Context.getContext().getDagDatas().stream()
                .filter(dag -> dag.getProcessDefinition().getCode() == depItem.getDefinitionCode())
                .map(dag -> dag.getProcessTaskRelationList())
                .flatMap(List::stream)
                .forEach(s -> {
                    List<Long> postCodes = relations.get(s.getPreTaskCode());
                    if (postCodes == null) {
                        relations.put(s.getPreTaskCode(), new ArrayList<>());
                    }
                    relations.get(s.getPreTaskCode()).add(s.getPostTaskCode());
                });
        return relations;
    }
}
