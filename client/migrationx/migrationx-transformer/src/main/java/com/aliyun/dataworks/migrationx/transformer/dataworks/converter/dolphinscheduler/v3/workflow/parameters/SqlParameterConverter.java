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
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.SpecRefEntity;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.DependencyType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableType;
import com.aliyun.dataworks.common.spec.domain.interfaces.Output;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.DbType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.sql.SqlParameters;
import com.aliyun.dataworks.migrationx.transformer.core.common.Constants;
import com.aliyun.dataworks.migrationx.transformer.core.utils.EmrCodeUtils;
import com.aliyun.migrationx.common.utils.BeanUtils;
import com.aliyun.migrationx.common.utils.GsonUtils;
import com.aliyun.migrationx.common.utils.UuidGenerators;

import com.google.gson.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;

@Slf4j
public class SqlParameterConverter extends AbstractParameterConverter<SqlParameters> {


    public SqlParameterConverter(Properties properties, SpecWorkflow specWorkflow, DagData processMeta, TaskDefinition taskDefinition) {
        super(properties, specWorkflow, processMeta, taskDefinition);
    }

    @Override
    protected void convertParameter(SpecNode specNode) {
        List<SpecVariable> specVariableList = convertSpecNodeParam(specNode);
        String sqlNodeMapStr = properties.getProperty(
                Constants.CONVERTER_TARGET_SQL_NODE_TYPE_MAP, "{}");
        Map<String, String> sqlTypeNodeTypeMapping = GsonUtils.fromJsonString(sqlNodeMapStr,
                new TypeToken<Map<String, String>>() {}.getType());

        String type = Optional.ofNullable(sqlTypeNodeTypeMapping)
                .map(s -> s.get(parameter.getType()))
                .orElseGet(() -> {
                    if (DbType.HIVE.name().equalsIgnoreCase(parameter.getType())) {
                        return CodeProgramType.EMR_HIVE.name();
                    } else if (DbType.SPARK.name().equalsIgnoreCase(parameter.getType())) {
                        return CodeProgramType.EMR_SPARK.name();
                    } else if (DbType.CLICKHOUSE.name().equalsIgnoreCase(parameter.getType())) {
                        return CodeProgramType.CLICK_SQL.name();
                    } else if (DbType.ofName(parameter.getType()) != null) {
                        return parameter.getType();
                    } else {
                        String defaultNodeTypeIfNotSupport = getSQLConverterType();
                        log.warn("using default node Type {} for node {}", defaultNodeTypeIfNotSupport, specNode.getName());
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

        //todo
        //SpecDatasource datasource = MapUtils.emptyIfNull(context.getDataSourceMap()).get(String.valueOf(parameter.getDatasource()));
        //if (Objects.nonNull(datasource)) {
        //    specNode.setDatasource(datasource);
        //}

        //dealPreAndPostSql(specNode, parameter.getPreStatements(), parameter.getPostStatements());
    }

    /**
     * parse pre sql and post sql to extra node
     *
     * @param specNodeRef origin node
     * @param preSqlList  pre sql list
     * @param postSqlList post sql list
     */
    private void dealPreAndPostSql(SpecNode specNodeRef, List<String> preSqlList, List<String> postSqlList) {
        List<SpecNode> specNodeList = new ArrayList<>();

        //if (CollectionUtils.isNotEmpty(preSqlList)) {
        //    for (int i = 0; i < preSqlList.size(); i++) {
        //        String sql = preSqlList.get(i);
        //        SpecNode preNode = copySpecNode(specNodeRef, sql, "_pre_" + i);
        //        specNodeList.add(preNode);
        //        if (i == 0) {
        //            headList.add(newWrapper(preNode));
        //        }
        //    }
        //}
        specNodeList.add(specNodeRef);
        //if (CollectionUtils.isNotEmpty(postSqlList)) {
        //    for (int i = 0; i < postSqlList.size(); i++) {
        //        String sql = postSqlList.get(i);
        //        SpecNode postNode = copySpecNode(specNodeRef, sql, "_post_" + i);
        //        specNodeList.add(postNode);
        //        if (i == postSqlList.size() - 1) {
        //            tailList.add(newWrapper(postNode));
        //        }
        //    }
        //}
        if (specNodeList.size() > 1) {
            SpecNode pre = specNodeList.get(0);
            for (int i = 1; i < specNodeList.size(); i++) {
                SpecNode specNode = specNodeList.get(i);
                specNode.getInputs().add(getDefaultOutput(pre));
                specNode.getInputs().addAll(getContextOutputs(pre));
                SpecFlowDepend specFlowDepend = newSpecFlowDepend();
                specFlowDepend.setNodeId(specNode);
                specFlowDepend.getDepends().add(new SpecDepend(pre, DependencyType.NORMAL, getDefaultOutput(pre)));
                getWorkflowDependencyList().add(specFlowDepend);
                pre = specNode;
            }
        }
    }

    /**
     * copy node, only used in pre and post sql.
     *
     * @param specNode origin node
     * @param sql      new sql
     * @param suffix   new suffix
     * @return copied node
     */
    private SpecNode copySpecNode(SpecNode specNode, String sql, String suffix) {
        SpecNode specNodeCopy = BeanUtils.deepCopy(specNode, SpecNode.class);
        specNodeCopy.setId(UuidGenerators.generateUuid());
        specNodeCopy.setName(specNodeCopy.getName() + suffix);
        for (Output output : specNodeCopy.getOutputs()) {
            if (output instanceof SpecNodeOutput && Boolean.TRUE.equals(((SpecNodeOutput) output).getIsDefault())) {
                ((SpecNodeOutput) output).setId(UuidGenerators.generateUuid());
                ((SpecNodeOutput) output).setData(specNodeCopy.getId());
                ((SpecNodeOutput) output).setRefTableName(specNodeCopy.getName());
            } else if (output instanceof SpecRefEntity) {
                ((SpecRefEntity) output).setId(UuidGenerators.generateUuid());
            }
        }
        specWorkflow.getNodes().add(specNodeCopy);

        SpecScript scriptCopy = BeanUtils.deepCopy(specNodeCopy.getScript(), SpecScript.class);
        scriptCopy.setId(UuidGenerators.generateUuid());
        scriptCopy.setPath(scriptCopy.getPath() + suffix);
        scriptCopy.setContent(sql);

        specNodeCopy.setScript(scriptCopy);
        return specNodeCopy;
    }

    private String getSQLConverterType() {
        String convertType = properties.getProperty(Constants.CONVERTER_TARGET_COMMAND_SQL_TYPE_AS);
        return getConverterType(convertType, CodeProgramType.SQL_COMPONENT.name());
    }
}
