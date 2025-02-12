package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.aliyun.dataworks.common.spec.domain.dw.types.CalcEngineType;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.dw.types.LabelType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.DataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.Project;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.UdfFunc;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.ResourceInfo;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.hivecli.HiveCliConstants;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.hivecli.HiveCliParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DwNode;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DwResource;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.types.ResourceType;
import com.aliyun.dataworks.migrationx.domain.dataworks.utils.DataStudioCodeUtils;
import com.aliyun.dataworks.migrationx.transformer.core.common.Constants;
import com.aliyun.dataworks.migrationx.transformer.core.utils.EmrCodeUtils;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.DolphinSchedulerConverterContext;
import com.aliyun.dataworks.migrationx.transformer.dataworks.transformer.DataWorksTransformerConfig;

import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class HiveCliParameterConverter extends AbstractParameterConverter<HiveCliParameters> {
    public HiveCliParameterConverter(DagData processMeta, TaskDefinition taskDefinition,
            DolphinSchedulerConverterContext<Project, DagData, DataSource, ResourceInfo,
                    UdfFunc> converterContext) {
        super(processMeta, taskDefinition, converterContext);
    }

    @Override
    public List<DwNode> convertParameter() throws IOException {
        String type;
        DwNode dwNode = newDwNode(taskDefinition);
        String executionType = parameter.getHiveCliTaskExecutionType();
        if (HiveCliConstants.TYPE_SCRIPT.equals(executionType)) {
            type = getScriptConverterType();
        } else {
            type = getConverterType();
        }
        if (!CodeProgramType.EMR_SHELL.name().equals(type) && !CodeProgramType.EMR_HIVE.name().equals(type)) {
            throw new RuntimeException("not support HIVECLI type with " + type);
        }
        dwNode.setType(type);
        Map<String, String> resourceMap = handleResourcesReference();
        List<String> resourceNames = new ArrayList<>();
        if (resourceMap != null) {
            resourceNames.addAll(resourceMap.values());
        }
        String code = buildCommand(this.parameter, dwNode, resourceNames);
        code = replaceCode(code, dwNode);
        code = replaceResourceFullName(resourceMap, code);
        dwNode.setCode(code);
        dwNode.setCode(EmrCodeUtils.toEmrCode(dwNode));
        return Arrays.asList(dwNode);
    }

    private String buildCommand(HiveCliParameters hiveCliParameters, DwNode dwNode, List<String> resourceNames) throws IOException {
        final String type = hiveCliParameters.getHiveCliTaskExecutionType();

        String resName = "";
        if (HiveCliConstants.TYPE_FILE.equals(type)) {
            List<ResourceInfo> resourceInfos = hiveCliParameters.getResourceList();
            if (resourceInfos != null && resourceInfos.size() > 0) {
                resName = resourceInfos.get(0).getResourceName();
            }
            resourceNames.add(resName);
        } else if (HiveCliConstants.TYPE_SCRIPT.equals(type)) {
            String sqlContent = hiveCliParameters.getHiveSqlScript();
            return sqlContent;
        } else {
            String sqlContent = hiveCliParameters.getHiveSqlScript();
            resName = generateSqlScriptFile(sqlContent);
            resourceNames.add(resName);
        }

        final List<String> args = new ArrayList<>();
        String resourceRef = DataStudioCodeUtils.addResourceReference(CodeProgramType.valueOf(dwNode.getType()), "", resourceNames);
        args.add(resourceRef + HiveCliConstants.HIVE_CLI_EXECUTE_FILE);
        args.add(resName);
        final String hiveCliOptions = hiveCliParameters.getHiveCliOptions();
        if (StringUtils.isNotEmpty(hiveCliOptions)) {
            args.add(hiveCliOptions);
        }

        String command = String.join(" ", args);
        return command;
    }

    private String generateSqlScriptFile(String rawScript) throws IOException {
        DwResource pyRes = new DwResource();
        pyRes.setName(Joiner.on("_").join(processMeta.getName(), taskDefinition.getName()) + ".sql");
        pyRes.setWorkflowRef(dwWorkflow);
        dwWorkflow.getResources().add(pyRes);

        List<String> paths = new ArrayList<>();
        DataWorksTransformerConfig config = DataWorksTransformerConfig.getConfig();
        if (config != null) {
            paths.add(CalcEngineType.EMR.getDisplayName(config.getLocale()));
            paths.add(LabelType.RESOURCE.getDisplayName(config.getLocale()));
        } else {
            paths.add(CalcEngineType.EMR.getDisplayName(Locale.SIMPLIFIED_CHINESE));
            paths.add(LabelType.RESOURCE.getDisplayName(Locale.SIMPLIFIED_CHINESE));
        }

        pyRes.setFolder(Joiner.on(File.separator).join(paths));
        pyRes.setType(CodeProgramType.EMR_FILE.name());
        pyRes.setExtend(ResourceType.FILE.name());

        File tmpFIle = new File(FileUtils.getTempDirectory(), pyRes.getName());
        FileUtils.writeStringToFile(tmpFIle, rawScript, StandardCharsets.UTF_8);
        pyRes.setLocalPath(tmpFIle.getAbsolutePath());

        return pyRes.getName();
    }

    private String getConverterType() {
        String convertType = properties.getProperty(Constants.CONVERTER_TARGET_HIVE_CLI_NODE_TYPE_AS);
        String defaultConvertType = CodeProgramType.EMR_SHELL.name();
        return getConverterType(convertType, defaultConvertType);
    }

    private String getScriptConverterType() {
        String convertType = properties.getProperty(Constants.CONVERTER_TARGET_HIVE_CLI_NODE_TYPE_AS);
        String defaultConvertType = CodeProgramType.EMR_HIVE.name();
        return getConverterType(convertType, defaultConvertType);
    }
}
