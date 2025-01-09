/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v1.workflow.parameters;

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
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.DolphinSchedulerV1Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.ProcessMeta;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.TaskNode;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.datasource.DataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.enums.DbType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.task.sql.SqlParameters;
import com.aliyun.dataworks.migrationx.transformer.core.common.Constants;
import com.aliyun.dataworks.migrationx.transformer.core.utils.EmrCodeUtils;
import com.aliyun.migrationx.common.utils.GsonUtils;
import com.aliyun.migrationx.common.utils.JSONUtils;

import com.google.gson.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang.StringUtils;

@Slf4j
public class SqlParameterConverter extends AbstractParameterConverter<SqlParameters> {

    public SqlParameterConverter(Properties properties, SpecWorkflow specWorkflow, ProcessMeta processMeta, TaskNode taskDefinition) {
        super(properties, specWorkflow, processMeta, taskDefinition);
    }

    @Override
    protected void convertParameter(SpecNode specNode) {
        List<SpecVariable> specVariableList = convertSpecNodeParam(specNode);
        String sqlNodeMapStr = properties.getProperty(
                Constants.CONVERTER_TARGET_SQL_NODE_TYPE_MAP, "{}");
        Map<String, String> sqlTypeNodeTypeMapping = GsonUtils.fromJsonString(sqlNodeMapStr,
                new TypeToken<Map<String, String>>() {}.getType());

        //String type = getSQLConverterType();
        String type = Optional.ofNullable(sqlTypeNodeTypeMapping)
                .map(s -> s.get(parameter.getType()))
                .orElseGet(() -> {
                    if (DbType.HIVE.equals(parameter.getType())) {
                        return CodeProgramType.EMR_HIVE.name();
                    } else if (DbType.SPARK.equals(parameter.getType())) {
                        return CodeProgramType.EMR_SPARK.name();
                    } else if (parameter.getType() != null) {
                        return parameter.getType().name();
                    } else {
                        String defaultNodeTypeIfNotSupport = getSQLConverterType();
                        log.warn("using default node Type {} for node {}", defaultNodeTypeIfNotSupport, taskDefinition.getName());
                        return defaultNodeTypeIfNotSupport;
                    }
                });

        CodeProgramType codeProgramType = CodeProgramType.getNodeTypeByName(type);
        SpecScript script = new SpecScript();
        String language = codeToLanguageIdentifier(codeProgramType);
        script.setLanguage(language);
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setEngine(codeProgramType.getCalcEngineType().getLabel());
        runtime.setCommand(codeProgramType.getName());
        script.setRuntime(runtime);

        script.setPath(getScriptPath(specNode));
        String content = parameter.getSql();

        if (CodeProgramType.EMR_HIVE.name().equals(codeProgramType) || CodeProgramType.EMR_SPARK.name().equals(codeProgramType)) {
            content = EmrCodeUtils.toEmrCode(codeProgramType, taskDefinition.getName(), content);
        }

        script.setContent(content);
        script.setParameters(ListUtils.emptyIfNull(specVariableList).stream().filter(v -> !VariableType.NODE_OUTPUT.equals(v.getType()))
                .collect(Collectors.toList()));
        specNode.setScript(script);
    }

    private String getConnectionName(String codeProgramType) {
        String mappingJson = properties.getProperty(Constants.WORKFLOW_CONVERTER_CONNECTION_MAPPING);
        if (StringUtils.isNotEmpty(mappingJson)) {
            Map<String, String> connectionMapping = JSONUtils.parseObject(mappingJson, Map.class);
            if (connectionMapping == null) {
                log.error("parse connection mapping with {} error", mappingJson);
            } else {
                String connectionName = connectionMapping.get(codeProgramType);
                log.info("Got connectionName {} by {}", connectionName, codeProgramType);
                return connectionName;
            }
        }

        if (!CodeProgramType.EMR_HIVE.name().equals(codeProgramType) && !CodeProgramType.EMR_SPARK.name().equals(codeProgramType)) {
            //add ref datasource
            List<DataSource> datasources = DolphinSchedulerV1Context.getContext().getDataSources();
            if (parameter.getDatasource() > 0) {
                return CollectionUtils.emptyIfNull(datasources).stream()
                        .filter(s -> s.getId() == parameter.getDatasource())
                        .findFirst()
                        .map(s -> s.getName())
                        .orElse(null);
            }
        }
        return null;
    }

    private String getSQLConverterType() {
        String convertType = properties.getProperty(Constants.CONVERTER_TARGET_COMMAND_SQL_TYPE_AS);
        return getConverterType(convertType, CodeProgramType.SQL_COMPONENT.name());
    }
}
