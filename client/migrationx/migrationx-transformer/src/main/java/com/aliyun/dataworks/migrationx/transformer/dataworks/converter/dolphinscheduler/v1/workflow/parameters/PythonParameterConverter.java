/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v1.workflow.parameters;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.ProcessMeta;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.TaskNode;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.task.python.PythonParameters;
import com.aliyun.dataworks.migrationx.transformer.core.common.Constants;

import org.apache.commons.collections4.ListUtils;

public class PythonParameterConverter extends AbstractParameterConverter<PythonParameters> {

    public PythonParameterConverter(Properties properties, SpecWorkflow specWorkflow, ProcessMeta processMeta, TaskNode taskDefinition) {
        super(properties, specWorkflow, processMeta, taskDefinition);
    }

    @Override
    protected void convertParameter(SpecNode specNode) {
        List<SpecVariable> specVariableList = convertSpecNodeParam(specNode);

        convertFileResourceList(specNode);
        String convertType = properties.getProperty(Constants.CONVERTER_TARGET_PYTHON_NODE_TYPE_AS, CodeProgramType.PYTHON.name());
        CodeProgramType codeProgramType = CodeProgramType.getNodeTypeByName(convertType);
        SpecScript script = new SpecScript();
        String language = codeToLanguageIdentifier(codeProgramType);
        script.setLanguage(language);
        //runtime
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setEngine(codeProgramType.getCalcEngineType().getLabel());
        runtime.setCommand(codeProgramType.getName());
        script.setRuntime(runtime);

        script.setPath(getScriptPath(specNode));

        String resourceReference = buildFileResourceReference(specNode, RESOURCE_REFERENCE_PREFIX);
        String pathImportCode = buildPathImportCode(specNode);
        script.setContent(resourceReference + pathImportCode + parameter.getRawScript());
        script.setParameters(ListUtils.emptyIfNull(specVariableList).stream().filter(v -> !VariableType.NODE_OUTPUT.equals(v.getType()))
                .collect(Collectors.toList()));
        specNode.setScript(script);
    }

    private String buildPathImportCode(SpecNode specNode) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("import os\n").append("import sys\n\n");
        Optional.ofNullable(specNode).map(SpecNode::getFileResources).ifPresent(fileResources ->
                fileResources.forEach(fileResource -> {
                    String fileName = fileResource.getName();
                    stringBuilder.append(String.format("sys.path.append(os.path.dirname(os.path.abspath('%s')))%n", fileName));
                }));
        stringBuilder.append("\n");
        return stringBuilder.toString();
    }
}
