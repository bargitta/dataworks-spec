package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DolphinSchedulerV3Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.DataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.Project;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.UdfFunc;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.DbType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.ResourceInfo;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.dlc.DLCParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DwNode;
import com.aliyun.dataworks.migrationx.transformer.core.common.Constants;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.DolphinSchedulerConverterContext;
import com.aliyun.migrationx.common.context.TransformerContext;
import com.aliyun.migrationx.common.utils.GsonUtils;
import com.aliyun.migrationx.common.utils.JSONUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.gson.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.SetUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @author 聿剑
 * @date 2022/10/18
 */
@Slf4j
public class DLCParameterConverter extends AbstractParameterConverter<DLCParameters> {

    private static final String CMD = "py2sql.py";

    private static final String TABLE_REGEX = "(?i)(INSERT)(\\s+)(OVERWRITE)(\\s+)(?!table\\b)([^ ]+)(\\s+)";
    private static final String TABLE_REPLACEMENT = "$1$2$3 TABLE $5$6";
    private static final Pattern TABLE_PATTERN = Pattern.compile(TABLE_REGEX);

    private final static String DATE_REPLACEMENT1 = "\\${p_date1}";
    private final static String DATE_param1 = "p_date1";
    private final static String DATE_REPLACEMENT2 = "\\${p_date2}";
    private final static String DATE_param2 = "p_date2";
    private final static String regex1 = "'\\$\\[yyyy-MM-dd-1\\]'";
    private final static String regex11 = "'\\$\\[yyyyMMdd-1\\]'";
    private final static String regex2 = "'\\$\\[yyyy-MM-dd-2\\]'";
    private final static String regex21 = "'\\$\\[yyyyMMdd-2\\]'";
    private final static Pattern DATE_PATTERN1 = Pattern.compile(regex1);
    private final static Pattern DATE_PATTERN11 = Pattern.compile(regex11);
    private final static Pattern DATE_PATTERN2 = Pattern.compile(regex2);
    private final static Pattern DATE_PATTERN21 = Pattern.compile(regex21);

    private static final Map<String, String> MAPPED_ENGINE = ImmutableMap.of(
            "da", "xingye_da",
            "dws_rec", "dws_reco",
            "ug", "xingye_ug",
            "risk_control", "mn_risk_control",
            "ad", "xingye_ad"
    );

    public DLCParameterConverter(DagData processMeta, TaskDefinition taskDefinition,
            DolphinSchedulerConverterContext<Project, DagData, DataSource, ResourceInfo,
                    UdfFunc> converterContext) {
        super(processMeta, taskDefinition, converterContext);
    }

    @Override
    public List<DwNode> convertParameter() {
        log.info("convert dlf parameter in task: {}", taskDefinition.getName());
        String sqlNodeMapStr = converterContext.getProperties().getProperty(
                Constants.CONVERTER_TARGET_SQL_NODE_TYPE_MAP, "{}");
        Map<String, String> sqlTypeNodeTypeMapping = GsonUtils.fromJsonString(sqlNodeMapStr,
                new TypeToken<Map<String, String>>() {}.getType());

        DwNode dwNode = newDwNode(taskDefinition);

        String codeProgramType = Optional.ofNullable(sqlTypeNodeTypeMapping)
                .map(s -> s.get(parameter.getType()))
                .orElseGet(() -> {
                    if (DbType.DLC.name().equalsIgnoreCase(parameter.getType())) {
                        return CodeProgramType.ODPS_SQL.name();
                    } else if (DbType.ofName(parameter.getType()) != null) {
                        return parameter.getType();
                    } else {
                        String defaultNodeTypeIfNotSupport = getSQLConverterType();
                        log.warn("using default node Type {} for node {}", defaultNodeTypeIfNotSupport, dwNode.getName());
                        return defaultNodeTypeIfNotSupport;
                    }
                });

        dwNode.setType(codeProgramType);
        Pair<Set<String>, String> paramCode = getCode();
        if (paramCode != null) {
            dwNode.setCode(paramCode == null ? null : paramCode.getRight());
            String params = Joiner.on(" ").join(SetUtils.emptyIfNull(paramCode.getLeft()));
            if (dwNode.getParameter() != null) {
                dwNode.setParameter(dwNode.getParameter() + " " + params);
            } else {
                dwNode.setParameter(params);
            }
        }

        dwNode.setConnection(getConnectionName(codeProgramType));
        log.info("successfully converted task parameter in task {}", taskDefinition.getName());
        return Collections.singletonList(dwNode);
    }

    private Pair<Set<String>, String> getCode() {
        if (isByFile()) {
            log.info("task {} read code from file", taskDefinition.getName());
            return getCodeByFile();
        } else {
            log.info("task {} read code from sql", taskDefinition.getName());
            String code = parameter.getSql();
            return handleSql(code);
        }
    }

    private Pair<Set<String>, String> handleSql(String sql) {
        if (StringUtils.isEmpty(sql)) {
            return null;
        }
        //add sql to comment
        String lines[] = sql.split("\\r?\\n");
        List<String> newLines = new ArrayList<>();
        for (String line : lines) {
            line = "-- " + line;
            newLines.add(line);
        }
        String commentedSql = StringUtils.join(newLines, " \n");
        //replace engine
        sql = replaceEngine(sql);
        //replace OVERWRITE
        sql = addOverwriteTableKeyWord(sql);
        //get parameters
        Pair<String, Set<String>> pair = replaceDateParam(sql);
        Set<String> params = pair.getRight();
        sql = pair.getLeft();

        sql = commentedSql + "\n\n" + "set odps.sql.hive.compatible=true;\n\n" + sql;
        return Pair.of(params, sql);
    }

    private String replaceEngine(String sql) {
        for (Map.Entry<String, String> entry : MAPPED_ENGINE.entrySet()) {
            String regex = String.format("([\\S]*)(\\s+)%s\\.([^ ]+)\\s+", entry.getKey());
            String replacement = String.format("$1 $2%s.$3 ", entry.getValue());
            Pattern pattern = Pattern.compile(regex);

            Matcher matcher = pattern.matcher(sql);
            sql = matcher.replaceAll(replacement);
        }
        return sql;
    }

    private String addOverwriteTableKeyWord(String sql) {
        Matcher matcher = TABLE_PATTERN.matcher(sql);
        return matcher.replaceAll(TABLE_REPLACEMENT);
    }

    private Pair<String, Set<String>> replaceDateParam(String sql) {
        Set<String> params = new HashSet<>();

        Matcher matcher1 = DATE_PATTERN1.matcher(sql);
        if (matcher1.find()) {
            sql = matcher1.replaceAll(DATE_REPLACEMENT1);
            params.add(DATE_param1 + "=$[yyyymmdd-1]");
        }

        Matcher matcher11 = DATE_PATTERN11.matcher(sql);
        if (matcher11.find()) {
            sql = matcher11.replaceAll(DATE_REPLACEMENT1);
            params.add(DATE_param1 + "=$[yyyymmdd-1]");
        }

        Matcher matcher2 = DATE_PATTERN2.matcher(sql);
        if (matcher2.find()) {
            sql = matcher2.replaceAll(DATE_REPLACEMENT2);
            params.add(DATE_param2 + "=$[yyyymmdd-2]");
        }

        Matcher matcher21 = DATE_PATTERN21.matcher(sql);
        if (matcher21.find()) {
            sql = matcher21.replaceAll(DATE_REPLACEMENT2);
            params.add(DATE_param2 + "=$[yyyymmdd-2]");
        }

        return Pair.of(sql, params);
    }

    private boolean isByFile() {
        return StringUtils.isBlank(parameter.getSql());
    }

    private Pair<Set<String>, String> getCodeByFile() {
        String fileName = taskDefinition.getName() + ".py";
        File dir = TransformerContext.getContext().getCustomResourceDir();
        if (dir == null) {
            log.error("resource dir null, skip dlc resource parse");
            dir = new File("sources");
        }
        if (!dir.exists()) {
            throw new RuntimeException(dir.getAbsolutePath() + " not exists");
        }
        for (File child : Objects.requireNonNull(dir.listFiles())) {
            // get file by name, then parse sql
            if (child.getName().equals(fileName)) {
                return readfile(child);
            }
        }
        return null;
    }

    private String getCmd() {
        String scriptDir = TransformerContext.getContext().getScriptDir();
        if (scriptDir == null) {
            scriptDir = "script/";
        }
        File file = new File(scriptDir);
        if (!file.exists()) {
            throw new RuntimeException("script dir " + file.getAbsolutePath() + " not exists");
        }
        return file.getAbsolutePath() + File.separator + CMD;
    }

    private Pair<Set<String>, String> readfile(File file) {
        String cmd = getCmd();
        log.info("dlf read file cmd {}, source {}", cmd, file.getAbsolutePath());
        ProcessBuilder processBuilder = new ProcessBuilder("python", cmd, file.getAbsolutePath());
        processBuilder.redirectErrorStream(true);
        try {
            Process process = processBuilder.start();
            List<String> lines = IOUtils.readLines(IOUtils.toBufferedInputStream(process.getInputStream()), StandardCharsets.UTF_8);
            log.info("parse python file {} with res: \n {}", file.getAbsolutePath(), JSONUtils.toJsonString(lines));
            log.info("waiting for process finished");
            int exitCode = process.waitFor();
            if (exitCode > 0) {
                log.error("python res with code {}", exitCode);
                throw new RuntimeException("exit with " + exitCode);
            }
            Set<String> parameters = new HashSet<>();
            String res;
            if (lines.size() >= 1) {
                TypeReference<List<String>> type = new TypeReference<List<String>>() {};
                parameters = JSONUtils.parseObject(lines.get(0), type)
                        .stream()
                        .collect(Collectors.toSet());
                res = String.join("\n", lines.subList(1, lines.size()));
            } else {
                res = String.join("\n", lines);
            }
            return Pair.of(parameters, res);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String getConnectionName(String codeProgramType) {
        String mappingJson = converterContext.getProperties().getProperty(Constants.WORKFLOW_CONVERTER_CONNECTION_MAPPING);
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
            List<DataSource> datasources = DolphinSchedulerV3Context.getContext().getDataSources();
            if (parameter.getDatasource() > 0 && CollectionUtils.isNotEmpty(datasources)) {
                return CollectionUtils.emptyIfNull(datasources).stream()
                        .filter(Objects::nonNull)
                        .filter(s -> s.getId() == parameter.getDatasource())
                        .findFirst()
                        .map(s -> s.getName())
                        .orElseGet(null);
            }
        }
        return null;
    }

    private String getSQLConverterType() {
        String convertType = properties.getProperty(Constants.CONVERTER_TARGET_COMMAND_SQL_TYPE_AS);
        return getConverterType(convertType, CodeProgramType.SQL_COMPONENT.name());
    }
}
