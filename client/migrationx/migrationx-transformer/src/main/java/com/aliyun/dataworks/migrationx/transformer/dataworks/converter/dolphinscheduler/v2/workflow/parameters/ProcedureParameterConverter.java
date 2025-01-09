/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v2.workflow.parameters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.DolphinSchedulerV2Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.entity.DataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.DbType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.task.procedure.ProcedureParameters;
import com.aliyun.dataworks.migrationx.transformer.core.common.Constants;
import com.aliyun.migrationx.common.utils.GsonUtils;

import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections4.ListUtils;

public class ProcedureParameterConverter extends AbstractParameterConverter<ProcedureParameters> {

    public ProcedureParameterConverter(Properties properties, SpecWorkflow specWorkflow, DagData processMeta, TaskDefinition taskDefinition) {
        super(properties, specWorkflow, processMeta, taskDefinition);
    }

    @Override
    protected void convertParameter(SpecNode specNode) {
        List<SpecVariable> specVariableList = convertSpecNodeParam(specNode);

        convertFileResourceList(specNode);
        String convertType = properties.getProperty(Constants.CONVERTER_TARGET_SHELL_NODE_TYPE_AS, CodeProgramType.DIDE_SHELL.name());
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
        //todo
        //String resourceReference = buildFileResourceReference(specNode, RESOURCE_REFERENCE_PREFIX);
        //script.setContent(resourceReference + parameter.getRawScript());
        script.setContent(getCode());
        script.setParameters(ListUtils.emptyIfNull(specVariableList).stream().filter(v -> !VariableType.NODE_OUTPUT.equals(v.getType()))
                .collect(Collectors.toList()));
        specNode.setScript(script);
    }

    public String getCode() {
        String sqlNodeMapStr = properties.getProperty(
                Constants.CONVERTER_TARGET_SQL_NODE_TYPE_MAP, "{}");
        Map<String, DbType> sqlTypeNodeTypeMapping = GsonUtils.fromJsonString(sqlNodeMapStr,
                new TypeToken<Map<String, DbType>>() {}.getType());
        sqlTypeNodeTypeMapping = Optional.ofNullable(sqlTypeNodeTypeMapping).orElse(new HashMap<>(1));

        String defaultNodeTypeIfNotSupport = getConverterType();

        DbType codeProgramType = sqlTypeNodeTypeMapping.get(parameter.getType());

        //add ref datasource
        if (parameter.getDatasource() > 0) {
            List<DataSource> datasources = DolphinSchedulerV2Context.getContext().getDataSources();

            //todo
            //CollectionUtils.emptyIfNull(datasources).stream()
            //        .filter(s -> s.getId() == parameter.getDatasource())
            //        .findFirst()
            //        .ifPresent(s -> dwNode.setConnection(s.getName()));
        }
        String code = parameter.getMethod();
        return code;
    }

    private String getConverterType() {
        String convertType = properties.getProperty(Constants.CONVERTER_TARGET_UNSUPPORTED_NODE_TYPE_AS);
        return getConverterType(convertType, CodeProgramType.VIRTUAL.name());
    }
}
