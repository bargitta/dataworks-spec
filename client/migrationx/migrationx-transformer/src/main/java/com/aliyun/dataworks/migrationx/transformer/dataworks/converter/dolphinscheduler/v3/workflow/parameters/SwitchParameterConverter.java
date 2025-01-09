/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableType;
import com.aliyun.dataworks.common.spec.domain.noref.SpecBranch;
import com.aliyun.dataworks.common.spec.domain.noref.SpecBranches;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DolphinSchedulerV3Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.SwitchResultVo;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.switchs.SwitchParameters;
import com.aliyun.migrationx.common.utils.JSONUtils;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;

@Slf4j
public class SwitchParameterConverter extends AbstractParameterConverter<SwitchParameters> {

    public SwitchParameterConverter(Properties properties, SpecWorkflow specWorkflow, DagData processMeta, TaskDefinition taskDefinition) {
        super(properties, specWorkflow, processMeta, taskDefinition);
    }

    @Override
    protected void convertParameter(SpecNode specNode) {
        List<SpecVariable> specVariableList = convertSpecNodeParam(specNode);
        CodeProgramType codeProgramType = CodeProgramType.CONTROLLER_BRANCH;
        SpecScript script = new SpecScript();
        String language = codeToLanguageIdentifier(codeProgramType);
        script.setLanguage(language);
        
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setEngine(codeProgramType.getCalcEngineType().getLabel());
        runtime.setCommand(codeProgramType.getName());
        script.setRuntime(runtime);
        script.setPath(getScriptPath(specNode));
        script.setParameters(ListUtils.emptyIfNull(specVariableList).stream().filter(v -> !VariableType.NODE_OUTPUT.equals(v.getType()))
                .collect(Collectors.toList()));
        specNode.setScript(script);
        List<SpecBranches> branchList = convertParameter();
        SpecBranch branch = new SpecBranch();
        branch.setBranches(branchList);
        specNode.setBranch(branch);
    }

    public List<SpecBranches> convertParameter() {
        JsonNode param = JSONUtils.parseObject(taskDefinition.getTaskParams(), JsonNode.class);
        SwitchParameters switchParameters = null;
        if (param.get("switchResult") != null) {
            switchParameters = JSONUtils.parseObject(param.get("switchResult"), SwitchParameters.class);
        }
        if (switchParameters == null || switchParameters.getDependTaskList() == null || switchParameters.getDependTaskList().isEmpty()) {
            log.warn("no dependence param {}", taskDefinition.getTaskParams());
            return null;
        }

        Long defaultNextCode = switchParameters.getNextNode();
        List<SpecBranches> branchList = new ArrayList<>();

        for (SwitchResultVo switchResultVo : switchParameters.getDependTaskList()) {
            String condition = switchResultVo.getCondition();
            //task code
            Long nextNodeCode = switchResultVo.getNextNode();
            TaskDefinition branchTask = DolphinSchedulerV3Context.getContext().getTaskCodeMap().get(nextNodeCode);
            if (branchTask == null) {
                continue;
            }
            ///
            SpecBranches b = new SpecBranches();
            //b.setDesc(switchResultVo);
            SpecNodeOutput output = new SpecNodeOutput();
            //When the dependent task ends， replace with specNode id
            output.setData(String.valueOf(branchTask.getCode()));
            b.setOutput(output);
            b.setWhen(condition);
            branchList.add(b);
        }
        return branchList;
    }

}