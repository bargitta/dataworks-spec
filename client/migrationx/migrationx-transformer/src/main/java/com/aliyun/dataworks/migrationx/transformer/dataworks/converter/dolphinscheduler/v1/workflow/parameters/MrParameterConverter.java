/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v1.workflow.parameters;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.dw.codemodel.EmrAllocationSpec;
import com.aliyun.dataworks.common.spec.domain.dw.codemodel.EmrCode;
import com.aliyun.dataworks.common.spec.domain.dw.codemodel.EmrLauncher;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.utils.MapReduceArgsUtils;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.DolphinSchedulerV1Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.ProcessMeta;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.TaskNode;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.entity.ResourceInfo;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.task.mr.MapReduceParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.utils.DataStudioCodeUtils;
import com.aliyun.dataworks.migrationx.transformer.core.common.Constants;
import com.aliyun.dataworks.migrationx.transformer.core.utils.EmrCodeUtils;
import com.aliyun.migrationx.common.utils.BeanUtils;

import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class MrParameterConverter extends AbstractParameterConverter<MapReduceParameters> {
    public static final String MR_YARN_QUEUE = "mapreduce.job.queuename";

    public MrParameterConverter(Properties properties, SpecWorkflow specWorkflow, ProcessMeta processMeta, TaskNode taskDefinition) {
        super(properties, specWorkflow, processMeta, taskDefinition);
    }

    @Override
    protected void convertParameter(SpecNode specNode) {
        List<SpecVariable> specVariableList = convertSpecNodeParam(specNode);

        convertFileResourceList(specNode);

        SpecScript script = new SpecScript();
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
        //todo
        //String resourceReference = buildFileResourceReference(specNode, RESOURCE_REFERENCE_PREFIX);
        String code = convertCode(codeProgramType);
        //script.setContent(resourceReference + parameter.getRawScript());
        script.setContent(code);
        script.setParameters(ListUtils.emptyIfNull(specVariableList).stream().filter(v -> !VariableType.NODE_OUTPUT.equals(v.getType()))
                .collect(Collectors.toList()));
        specNode.setScript(script);
    }

    public String convertCode(CodeProgramType codeProgramType) {
        DolphinSchedulerV1Context context = DolphinSchedulerV1Context.getContext();
        List<ResourceInfo> resourceInfos = context.getResources();

        Optional.ofNullable(parameter).map(MapReduceParameters::getMainJar)
                .flatMap(mainJar -> ListUtils.emptyIfNull(resourceInfos)
                        .stream().filter(res -> Objects.equals(res.getId(), mainJar.getId()))
                        .findFirst()).ifPresent(res -> parameter.setMainJar(res));

        ListUtils.emptyIfNull(Optional.ofNullable(parameter).map(MapReduceParameters::getResourceFilesList)
                        .orElse(ListUtils.emptyIfNull(null)))
                .forEach(res -> ListUtils.emptyIfNull(resourceInfos).stream()
                        .filter(res1 -> Objects.equals(res1.getId(), res.getId()))
                        .forEach(res1 -> BeanUtils.copyProperties(res1, res)));

        String type = codeProgramType.getName();

        List<String> resources = ListUtils.emptyIfNull(parameter.getResourceFilesList()).stream()
                .filter(Objects::nonNull)
                .map(ResourceInfo::getName).distinct().collect(Collectors.toList());

        List<String> codeLines = new ArrayList<>();
        codeLines.add(DataStudioCodeUtils.addResourceReference(CodeProgramType.valueOf(type), "", resources));

        // convert to EMR_MR
        if (StringUtils.equalsIgnoreCase(CodeProgramType.EMR_MR.name(), type)) {
            String command = Joiner.on(" ").join(MapReduceArgsUtils.buildArgs(parameter).stream()
                    .map(String::valueOf).collect(Collectors.toList()));
            codeLines.add(command);

            String code = Joiner.on("\n").join(codeLines);
            code = EmrCodeUtils.toEmrCode(codeProgramType, taskDefinition.getName(), code);
            EmrCode emrCode = EmrCodeUtils.asEmrCode(type, code);
            Optional.ofNullable(emrCode).map(EmrCode::getLauncher)
                    .map(EmrLauncher::getAllocationSpec)
                    .map(EmrAllocationSpec::of)
                    .ifPresent(spec -> {
                        spec.setQueue(parameter.getQueue());
                        emrCode.getLauncher().setAllocationSpec(spec.toMap());
                    });
            return emrCode.getContent();
        }

        // convert to ODPS_MR
        if (StringUtils.equalsIgnoreCase(CodeProgramType.ODPS_MR.name(), type)) {
            String command = Joiner.on(" ").join(
                    "jar", "-resources",
                    Optional.ofNullable(parameter.getMainJar().getName()).orElse(""),
                    "-classpath",
                    Joiner.on(",").join(resources),
                    Optional.ofNullable(parameter.getMainClass()).orElse(""),
                    Optional.ofNullable(parameter.getMainArgs()).orElse(""),
                    Optional.ofNullable(parameter.getOthers()).orElse("")
            );
            codeLines.add(command);

            String code = Joiner.on("\n").join(codeLines);
            code = EmrCodeUtils.toEmrCode(codeProgramType, taskDefinition.getName(), code);
            EmrCode emrCode = EmrCodeUtils.asEmrCode(type, code);
            Optional.ofNullable(emrCode).map(EmrCode::getLauncher)
                    .map(EmrLauncher::getAllocationSpec)
                    .map(EmrAllocationSpec::of)
                    .ifPresent(spec -> {
                        spec.setQueue(parameter.getQueue());
                        emrCode.getLauncher().setAllocationSpec(spec.toMap());
                    });
            return emrCode.getContent();
        }

        throw new RuntimeException("Unsupported code type: " + type);
    }

    private String getConverterType() {
        String convertType = properties.getProperty(Constants.CONVERTER_TARGET_MR_NODE_TYPE_AS);
        String defaultConvertType = CodeProgramType.EMR_MR.name();
        return getConverterType(convertType, defaultConvertType);
    }
}
