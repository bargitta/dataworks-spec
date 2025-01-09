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
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.ResourceInfo;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.hivecli.HiveCliConstants;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.hivecli.HiveCliParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.utils.DataStudioCodeUtils;
import com.aliyun.dataworks.migrationx.transformer.core.common.Constants;
import com.aliyun.dataworks.migrationx.transformer.core.utils.EmrCodeUtils;

import com.google.common.base.Joiner;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;

public class HiveCliParameterConverter extends AbstractParameterConverter<HiveCliParameters> {
    public HiveCliParameterConverter(Properties properties, SpecWorkflow specWorkflow, DagData processMeta, TaskDefinition taskDefinition) {
        super(properties, specWorkflow, processMeta, taskDefinition);
    }

    @Override
    protected void convertParameter(SpecNode specNode) {
        List<SpecVariable> specVariableList = convertSpecNodeParam(specNode);

        convertFileResourceList(specNode);

        SpecScript script = new SpecScript();
        //script.setId(generateUuid());
        String type = getConverterType();
        CodeProgramType codeProgramType = CodeProgramType.getNodeTypeByName(type);
        String language = codeToLanguageIdentifier(codeProgramType);
        script.setLanguage(language);
        //runtime
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setEngine(codeProgramType.getCalcEngineType().getLabel());
        runtime.setCommand(codeProgramType.getName());
        script.setRuntime(runtime);

        script.setPath(getScriptPath(specNode));
        //String resourceReference = buildFileResourceReference(specNode, RESOURCE_REFERENCE_PREFIX);
        String cmd = buildCommand(codeProgramType, this.parameter);
        String code = EmrCodeUtils.toEmrCode(codeProgramType, taskDefinition.getName(), cmd);
        String resourceReference = buildFileResourceReference(specNode, RESOURCE_REFERENCE_PREFIX);

        //String code = convertCode(codeProgramType, taskDefinition.getName());
        script.setContent(resourceReference + code);
        script.setContent(code);
        script.setParameters(ListUtils.emptyIfNull(specVariableList).stream().filter(v -> !VariableType.NODE_OUTPUT.equals(v.getType()))
                .collect(Collectors.toList()));
        specNode.setScript(script);
    }

    protected String buildFileResourceReference(SpecNode specNode, String prefix) {
        StringBuilder stringBuilder = new StringBuilder();
        Optional.ofNullable(specNode).map(SpecNode::getFileResources)
                .ifPresent(fileResources ->
                        fileResources.forEach(fileResource ->
                                stringBuilder.append(String.format(RESOURCE_REFERENCE_FORMAT, prefix, fileResource.getName())).append("\n")));
        return stringBuilder.append("\n").toString();
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

    private String buildCommand(CodeProgramType codeProgramType, HiveCliParameters hiveCliParameters) {
        final String type = hiveCliParameters.getHiveCliTaskExecutionType();

        String resName = "";
        if (HiveCliConstants.TYPE_FILE.equals(type)) {
            List<ResourceInfo> resourceInfos = hiveCliParameters.getResourceList();
            if (resourceInfos != null && resourceInfos.size() > 0) {
                resName = resourceInfos.get(0).getResourceName();
            }
        } else {
            String sqlContent = hiveCliParameters.getHiveSqlScript();
            resName = Joiner.on("_").join(processDefinition.getName(), taskDefinition.getName()) + ".sql";
        }

        final List<String> args = new ArrayList<>();
        List<String> resources = new ArrayList<>();
        resources.add(resName);
        String resourceRef = DataStudioCodeUtils.addResourceReference(codeProgramType, "", resources);
        args.add(resourceRef + HiveCliConstants.HIVE_CLI_EXECUTE_FILE);
        args.add(resName);
        final String hiveCliOptions = hiveCliParameters.getHiveCliOptions();
        if (StringUtils.isNotEmpty(hiveCliOptions)) {
            args.add(hiveCliOptions);
        }

        String command = String.join(" ", args);

        return command;
    }

    private String getConverterType() {
        String convertType = properties.getProperty(Constants.CONVERTER_TARGET_SHELL_NODE_TYPE_AS);
        String defaultConvertType = CodeProgramType.EMR_SHELL.name();
        return getConverterType(convertType, defaultConvertType);
    }
}
