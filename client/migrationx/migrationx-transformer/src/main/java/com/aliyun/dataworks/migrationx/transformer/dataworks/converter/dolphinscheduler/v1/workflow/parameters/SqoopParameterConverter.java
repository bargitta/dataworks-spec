/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v1.workflow.parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.entity.ResourceInfo;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.task.sqoop.SqoopParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.task.sqoop.sources.SourceHdfsParameter;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.task.sqoop.sources.SourceHiveParameter;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.task.sqoop.sources.SourceMysqlParameter;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.task.sqoop.targets.TargetHdfsParameter;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.task.sqoop.targets.TargetHiveParameter;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.task.sqoop.targets.TargetMysqlParameter;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.types.CodeModeType;
import com.aliyun.dataworks.migrationx.transformer.core.sqoop.DIConfigTemplate;
import com.aliyun.dataworks.migrationx.transformer.core.sqoop.DIJsonProcessor;
import com.aliyun.migrationx.common.utils.JSONUtils;

import com.google.common.base.Joiner;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;

public class SqoopParameterConverter extends AbstractParameterConverter<SqoopParameters> {

    public SqoopParameterConverter(Properties properties, SpecWorkflow specWorkflow, ProcessMeta processMeta, TaskNode taskDefinition) {
        super(properties, specWorkflow, processMeta, taskDefinition);
    }

    @Override
    protected void convertParameter(SpecNode specNode) {
        List<SpecVariable> specVariableList = convertSpecNodeParam(specNode);
        CodeProgramType codeProgramType = CodeProgramType.DI;

        SpecScript script = new SpecScript();
        String language = codeToLanguageIdentifier(codeProgramType);
        script.setLanguage(language);

        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setEngine(codeProgramType.getCalcEngineType().getLabel());
        runtime.setCommand(codeProgramType.getName());
        script.setRuntime(runtime);

        script.setPath(getScriptPath(specNode));
        String content = convertCode();
        script.setContent(content);
        //todo
        //String codeMode = CodeModeType.CODE.getValue();

        script.setParameters(ListUtils.emptyIfNull(specVariableList).stream().filter(v -> !VariableType.NODE_OUTPUT.equals(v.getType()))
                .collect(Collectors.toList()));
        specNode.setScript(script);
    }

    public String convertCode() {
        DIJsonProcessor diProcessor = DIJsonProcessor.from(DIConfigTemplate.DI_CODE_TEMPLATE);
        DIJsonProcessor readerJp = diProcessor.getConfiguration("steps[0]");
        DIJsonProcessor writerJp = diProcessor.getConfiguration("steps[1]");
        processSqoopSource(readerJp);
        processSqoopTarget(writerJp);
        diProcessor.set("extend.mode", CodeModeType.CODE.getValue());
        return diProcessor.toJSON();
    }

    private void processSqoopTarget(DIJsonProcessor writerJp) {
        DolphinSchedulerV1Context context = DolphinSchedulerV1Context.getContext();
        List<ResourceInfo> resourceInfos = context.getResources();

        writerJp.set("stepType", StringUtils.lowerCase(parameter.getTargetType()));
        if (StringUtils.equalsIgnoreCase(parameter.getTargetType(), "mysql")) {
            TargetMysqlParameter targetMysqlParameter = JSONUtils.parseObject(parameter.getTargetParams(),
                    TargetMysqlParameter.class);
            Optional.ofNullable(targetMysqlParameter).ifPresent(p -> {
                ListUtils.emptyIfNull(resourceInfos).stream()
                        .filter(ds -> Objects.equals(ds.getId(), targetMysqlParameter.getTargetDatasource()))
                        .findFirst().ifPresent(ds -> writerJp.set("parameter.datasource", ds.getName()));
                writerJp.set("parameter.table", targetMysqlParameter.getTargetTable());
                writerJp.set("parameter.preSql", Optional.ofNullable(StringUtils.split(
                        targetMysqlParameter.getPreQuery(), ",")).orElse(new String[]{}));
                writerJp.set("parameter.column", StringUtils.isBlank(targetMysqlParameter.getTargetColumns()) ?
                        new String[]{"*"} : StringUtils.split(targetMysqlParameter.getTargetColumns(), ","));
                if (StringUtils.equalsIgnoreCase(targetMysqlParameter.getTargetUpdateMode(), "updateonly")) {
                    writerJp.set("parameter.writeMode", "update");
                } else {
                    writerJp.set("parameter.writeMode", "replace");
                }
            });
        }

        if (StringUtils.equalsIgnoreCase(parameter.getTargetType(), "hive")) {
            TargetHiveParameter targetHiveParameter = JSONUtils.parseObject(parameter.getTargetParams(),
                    TargetHiveParameter.class);
            Optional.ofNullable(targetHiveParameter).ifPresent(p -> {
                ListUtils.emptyIfNull(resourceInfos).stream()
                        .filter(ds -> StringUtils.equals(java.lang.String.valueOf(ds.getId()), targetHiveParameter.getHiveDatabase()))
                        .findFirst().ifPresent(ds -> writerJp.set("parameter.datasource", ds.getName()));
                writerJp.set("parameter.table", targetHiveParameter.getHiveTable());
                String[] keys = Optional.ofNullable(StringUtils.split(
                        targetHiveParameter.getHivePartitionKey(), ",")).orElse(new String[]{});
                String[] values = Optional.ofNullable(StringUtils.split(
                        targetHiveParameter.getHivePartitionValue(), ",")).orElse(new String[]{});
                List<String> partitions = new ArrayList<>();
                for (int i = 0; i < Math.min(keys.length, values.length); i++) {
                    partitions.add(Joiner.on("=").join(keys[i], values[i]));
                }
                writerJp.set("parameter.partition", Joiner.on(",").join(partitions));
                writerJp.set("parameter.hdfsUsername", "hdfs");
                writerJp.set("parameter.writeMode", "append");
            });
        }

        if (StringUtils.equalsIgnoreCase(parameter.getTargetType(), "hdfs")) {
            TargetHdfsParameter targetHdfsParameter = JSONUtils.parseObject(parameter.getSourceParams(),
                    TargetHdfsParameter.class);
            Optional.ofNullable(targetHdfsParameter).ifPresent(p -> {
                writerJp.set("parameter.path", p.getTargetPath());
                writerJp.set("parameter.compress", targetHdfsParameter.getCompressionCodec());
                writerJp.set("parameter.datasource", "hdfs");
                writerJp.set("parameter.fileType", targetHdfsParameter.getFileType());
                if (StringUtils.equalsIgnoreCase("parquet", targetHdfsParameter.getFileType())) {
                    writerJp.set("parameter.writeMode", "noConflict");
                } else {
                    writerJp.set("parameter.writeMode", "append");
                }
            });
        }
    }

    private void processSqoopSource(DIJsonProcessor readerJp) {
        DolphinSchedulerV1Context context = DolphinSchedulerV1Context.getContext();
        List<ResourceInfo> resourceInfos = context.getResources();

        readerJp.set("stepType", StringUtils.lowerCase(parameter.getSourceType()));
        if (StringUtils.equalsIgnoreCase(parameter.getSourceType(), "mysql")) {
            SourceMysqlParameter mysqlParameter = JSONUtils.parseObject(parameter.getSourceParams(),
                    SourceMysqlParameter.class);
            Optional.ofNullable(mysqlParameter).ifPresent(p -> {
                ListUtils.emptyIfNull(resourceInfos).stream()
                        .filter(ds -> Objects.equals(ds.getId(), mysqlParameter.getSrcDatasource()))
                        .findFirst().ifPresent(ds -> readerJp.set("parameter.datasource", ds.getName()));
                readerJp.set("parameter.table", mysqlParameter.getSrcTable());
                readerJp.set("parameter.where", StringUtils.substringAfter(
                        StringUtils.lowerCase(mysqlParameter.getSrcQuerySql()), "where"));
                readerJp.set("parameter.column", StringUtils.isBlank(mysqlParameter.getSrcColumns()) ?
                        new String[]{"*"} : StringUtils.split(mysqlParameter.getSrcColumns(), ","));
            });
        }

        if (StringUtils.equalsIgnoreCase(parameter.getSourceType(), "hive")) {
            SourceHiveParameter sourceHiveParameter = JSONUtils.parseObject(parameter.getSourceParams(),
                    SourceHiveParameter.class);
            Optional.ofNullable(sourceHiveParameter).ifPresent(p -> {
                ListUtils.emptyIfNull(resourceInfos).stream()
                        .filter(ds -> StringUtils.equals(String.valueOf(ds.getId()), sourceHiveParameter.getHiveDatabase()))
                        .findFirst().ifPresent(ds -> readerJp.set("parameter.datasource", ds.getName()));
                readerJp.set("parameter.table", sourceHiveParameter.getHiveTable());
                String[] keys = Optional.ofNullable(StringUtils.split(
                        sourceHiveParameter.getHivePartitionKey(), ",")).orElse(new String[]{});
                String[] values = Optional.ofNullable(StringUtils.split(
                        sourceHiveParameter.getHivePartitionValue(), ",")).orElse(new String[]{});
                List<String> partitions = new ArrayList<>();
                for (int i = 0; i < Math.min(keys.length, values.length); i++) {
                    partitions.add(Joiner.on("=").join(keys[i], values[i]));
                }
                readerJp.set("parameter.partition", Joiner.on(",").join(partitions));
                readerJp.set("parameter.readMode", "hdfs");
                readerJp.set("parameter.hdfsUsername", "hdfs");
                List<Map<String, String>> columns = Arrays.stream(keys).map(key -> {
                    Map<String, String> column = new HashMap<>();
                    column.put("type", "string");
                    column.put("value", key);
                    return column;
                }).collect(Collectors.toList());
                readerJp.set("parameter.hivePartitionColumn", columns);
            });
        }

        if (StringUtils.equalsIgnoreCase(parameter.getSourceType(), "hdfs")) {
            SourceHdfsParameter sourceHdfsParameter = JSONUtils.parseObject(parameter.getSourceParams(),
                    SourceHdfsParameter.class);
            Optional.ofNullable(sourceHdfsParameter).ifPresent(
                    p -> readerJp.set("parameter.path", p.getExportDir()));
        }
    }
}
