/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v2.workflow.parameters;

import java.util.Arrays;
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
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.DolphinSchedulerV2Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.entity.DataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.DbType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.Flag;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.process.Property;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.task.datax.DataxParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.task.datax.DataxUtils;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.utils.ParameterUtils;
import com.aliyun.dataworks.migrationx.transformer.core.common.Constants;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.entity.Connection;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.entity.Parameter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.entity.Step;
import com.aliyun.migrationx.common.utils.JSONUtils;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class DataxParameterConverter extends AbstractParameterConverter<DataxParameters> {
    /**
     * select all
     */
    private static final String SELECT_ALL_CHARACTER = "*";

    private static final int DATAX_CHANNEL_COUNT = 1;

    public DataxParameterConverter(Properties properties, SpecWorkflow specWorkflow, DagData processMeta, TaskDefinition taskDefinition) {
        super(properties, specWorkflow, processMeta, taskDefinition);
    }

    @Override
    protected void convertParameter(SpecNode specNode) {
        List<SpecVariable> specVariableList = convertSpecNodeParam(specNode);
        String type = properties.getProperty(Constants.CONVERTER_TARGET_DATAX_NODE_TYPE_AS, CodeProgramType.DI.name());
        CodeProgramType codeProgramType = CodeProgramType.getNodeTypeByName(type);
        SpecScript script = new SpecScript();
        String language = codeToLanguageIdentifier(codeProgramType);
        script.setLanguage(language);
        //runtime
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setEngine(codeProgramType.getCalcEngineType().getLabel());
        runtime.setCommand(codeProgramType.getName());
        script.setRuntime(runtime);

        script.setPath(getScriptPath(specNode));
        String json = buildDataxJsonFile(new HashMap<>());
        script.setContent(json);
        script.setParameters(ListUtils.emptyIfNull(specVariableList).stream().filter(v -> !VariableType.NODE_OUTPUT.equals(v.getType()))
                .collect(Collectors.toList()));
        specNode.setScript(script);
    }

    private String buildDataxJsonFile(Map<String, Property> paramsMap) {
        String json;

        if (parameter.getCustomConfig() == Flag.YES.ordinal()) {
            json = parameter.getJson().replaceAll("\\r\\n", System.lineSeparator());
        } else {
            ObjectNode job = JSONUtils.createObjectNode();
            job.put("transform", false);
            job.put("type", "job");
            job.put("version", "2.0");
            ArrayNode steps = buildDataxJobSteps();
            job.putArray("steps").addAll(steps);
            job.set("setting", buildDataxJobSettingJson());
            json = job.toString();
        }

        // replace placeholder
        json = ParameterUtils.convertParameterPlaceholders(json, ParameterUtils.convert(paramsMap));

        log.debug("datax job json : {}", json);
        return json;
    }

    private ArrayNode buildDataxJobSteps() {
        DataSource source = getDataSource(parameter.getDataSource());
        DataSource target = getDataSource(parameter.getDataTarget());
        Step reader = new Step();
        Step writer = new Step();
        reader.setName("Reader");
        reader.setCategory("reader");
        reader.setStepType(source.getType().name().toLowerCase());

        writer.setName("Writer");
        writer.setCategory("writer");
        writer.setStepType(target.getType().name().toLowerCase());

        Parameter readParameter = new Parameter();
        readParameter.setEncoding("UTF-8");
        readParameter.setEnvType(1);
        readParameter.setUseSpecialSecret(false);

        Parameter writeParameter = new Parameter();
        writeParameter.setEncoding("UTF-8");
        writeParameter.setEnvType(1);
        writeParameter.setUseSpecialSecret(false);

        Pair<String, String[]> pair = tryGrammaticalAnalysisSqlColumnNames(source.getType(), parameter.getSql());
        String[] srcColumnNames = pair.getRight();
        if (srcColumnNames != null && srcColumnNames.length > 0) {
            readParameter.setColumn(Arrays.asList(srcColumnNames));
            String[] tgtColumnNames = DataxUtils.convertKeywordsColumns(target.getType(), srcColumnNames);
            if (tgtColumnNames != null && tgtColumnNames.length > 0) {
                writeParameter.setColumn(Arrays.asList(srcColumnNames));
            }
        }
        Connection sourceConnection = new Connection();
        sourceConnection.setDatasource(source.getName());
        sourceConnection.setTable(Arrays.asList(pair.getLeft()));
        readParameter.setConnection(Arrays.asList(sourceConnection));
        Connection targetConnection = new Connection();
        targetConnection.setDatasource(target.getName());
        targetConnection.setTable(Arrays.asList(parameter.getTargetTable()));
        writeParameter.setConnection(Arrays.asList(targetConnection));
        reader.setParameter(readParameter);
        writer.setParameter(writeParameter);

        ObjectNode steps = JSONUtils.createObjectNode();
        ArrayNode tableArr = steps.putArray("steps");
        JsonNode readerNode = JSONUtils.toJsonNode(reader);
        tableArr.add(readerNode);
        JsonNode writerNode = JSONUtils.toJsonNode(writer);
        tableArr.add(writerNode);
        return tableArr;
    }

    /**
     * build datax setting config
     *
     * @return datax setting config JSONObject
     */
    private ObjectNode buildDataxJobSettingJson() {

        ObjectNode speed = JSONUtils.createObjectNode();

        speed.put("channel", DATAX_CHANNEL_COUNT);

        if (parameter.getJobSpeedByte() > 0) {
            speed.put("byte", parameter.getJobSpeedByte());
        }

        if (parameter.getJobSpeedRecord() > 0) {
            speed.put("record", parameter.getJobSpeedRecord());
        }

        ObjectNode errorLimit = JSONUtils.createObjectNode();
        errorLimit.put("record", 0);
        errorLimit.put("percentage", 0);

        ObjectNode setting = JSONUtils.createObjectNode();
        setting.set("speed", speed);
        setting.set("errorLimit", errorLimit);

        return setting;
    }

    /**
     * try grammatical parsing column
     *
     * @param dbType database type
     * @param sql    sql for data synchronization
     * @return column name array
     * @throws RuntimeException if error throws RuntimeException
     */
    private Pair<String, String[]> tryGrammaticalAnalysisSqlColumnNames(DbType dbType, String sql) {
        String[] columnNames;
        String sourceTable = null;
        try {
            SQLStatementParser parser = DataxUtils.getSqlStatementParser(dbType, sql);
            if (parser == null) {
                log.warn("database driver [{}] is not support grammatical analysis sql", dbType);
                return Pair.of(null, new String[0]);
            }

            SQLStatement sqlStatement = parser.parseStatement();
            SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) sqlStatement;
            SQLSelect sqlSelect = sqlSelectStatement.getSelect();

            List<SQLSelectItem> selectItemList = null;

            if (sqlSelect.getQuery() instanceof SQLSelectQueryBlock) {
                SQLSelectQueryBlock block = (SQLSelectQueryBlock) sqlSelect.getQuery();
                selectItemList = block.getSelectList();
                if (block.getFrom() instanceof SQLExprTableSource) {
                    SQLExprTableSource expr = (SQLExprTableSource) block.getFrom();
                    if (expr.getExpr() instanceof SQLIdentifierExpr) {
                        sourceTable = ((SQLIdentifierExpr) expr.getExpr()).getName();
                    }
                }
            } else if (sqlSelect.getQuery() instanceof SQLUnionQuery) {
                SQLUnionQuery unionQuery = (SQLUnionQuery) sqlSelect.getQuery();
                SQLSelectQueryBlock block = (SQLSelectQueryBlock) unionQuery.getRight();
                selectItemList = block.getSelectList();
                if (block.getFrom() instanceof SQLExprTableSource) {
                    SQLExprTableSource expr = (SQLExprTableSource) block.getFrom();
                    if (expr.getExpr() instanceof SQLIdentifierExpr) {
                        sourceTable = ((SQLIdentifierExpr) expr.getExpr()).getName();
                    }
                }
            }

            if (selectItemList == null) {
                throw new RuntimeException(String.format("select query type [%s] is not support", sqlSelect.getQuery().toString()));
            }

            columnNames = new String[selectItemList.size()];
            for (int i = 0; i < selectItemList.size(); i++) {
                SQLSelectItem item = selectItemList.get(i);

                String columnName = null;

                if (item.getAlias() != null) {
                    columnName = item.getAlias();
                } else if (item.getExpr() != null) {
                    if (item.getExpr() instanceof SQLPropertyExpr) {
                        SQLPropertyExpr expr = (SQLPropertyExpr) item.getExpr();
                        columnName = expr.getName();
                    } else if (item.getExpr() instanceof SQLIdentifierExpr) {
                        SQLIdentifierExpr expr = (SQLIdentifierExpr) item.getExpr();
                        columnName = expr.getName();
                    }
                } else {
                    throw new RuntimeException(
                            String.format("grammatical analysis sql column [ %s ] failed", item));
                }

                if (SELECT_ALL_CHARACTER.equals(item.toString())) {
                    log.info("sql contains *, grammatical analysis failed");
                    return Pair.of(sourceTable, new String[]{"*"});
                }

                if (columnName == null) {
                    throw new RuntimeException(
                            String.format("grammatical analysis sql column [ %s ] failed", item));
                }

                columnNames[i] = columnName;
            }
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            return Pair.of(null, new String[0]);
        }

        return Pair.of(sourceTable, columnNames);
    }

    private DataSource getDataSource(int datasourceId) {
        List<DataSource> datasources = DolphinSchedulerV2Context.getContext().getDataSources();
        return CollectionUtils.emptyIfNull(datasources).stream()
                .filter(s -> s.getId() == datasourceId)
                .findFirst()
                .orElse(null);
    }
}
