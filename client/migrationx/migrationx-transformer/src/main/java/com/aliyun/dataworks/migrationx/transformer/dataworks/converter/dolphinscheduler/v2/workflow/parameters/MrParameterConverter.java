/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v2.workflow.parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.utils.ArgsUtils;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.DolphinSchedulerV2Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.ProgramType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.process.ResourceInfo;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.task.mr.MapReduceParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.utils.ParameterUtils;
import com.aliyun.dataworks.migrationx.domain.dataworks.utils.DataStudioCodeUtils;
import com.aliyun.dataworks.migrationx.transformer.core.common.Constants;
import com.aliyun.dataworks.migrationx.transformer.core.utils.EmrCodeUtils;
import com.aliyun.migrationx.common.utils.BeanUtils;

import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.task.mr.MapReduceTaskConstants.MR_NAME;
import static com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.task.mr.MapReduceTaskConstants.MR_QUEUE;
import static com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.utils.TaskConstants.D;

@Slf4j
public class MrParameterConverter extends AbstractParameterConverter<MapReduceParameters> {
    public static final String MR_YARN_QUEUE = "mapreduce.job.queuename";

    public MrParameterConverter(Properties properties, SpecWorkflow specWorkflow, DagData processMeta, TaskDefinition taskDefinition) {
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
        String code = convertCode(codeProgramType, taskDefinition.getName());
        //script.setContent(resourceReference + parameter.getRawScript());
        script.setContent(code);
        script.setParameters(ListUtils.emptyIfNull(specVariableList).stream().filter(v -> !VariableType.NODE_OUTPUT.equals(v.getType()))
                .collect(Collectors.toList()));
        specNode.setScript(script);
    }

    public String convertCode(CodeProgramType codeProgramType, String taskName) {
        List<ResourceInfo> resourceInfos = DolphinSchedulerV2Context.getContext().getResources();

        Optional.ofNullable(parameter).map(MapReduceParameters::getMainJar)
                .flatMap(mainJar -> ListUtils.emptyIfNull(resourceInfos)
                        .stream().filter(res -> Objects.equals(res.getId(), mainJar.getId()))
                        .findFirst()).ifPresent(res -> parameter.setMainJar(res));

        ListUtils.emptyIfNull(Optional.ofNullable(parameter).map(MapReduceParameters::getResourceFilesList)
                        .orElse(ListUtils.emptyIfNull(null)))
                .forEach(res -> ListUtils.emptyIfNull(resourceInfos).stream()
                        .filter(res1 -> Objects.equals(res1.getId(), res.getId()))
                        .forEach(res1 -> BeanUtils.copyProperties(res1, res)));

        // convert to EMR_MR
        if (StringUtils.equalsIgnoreCase(CodeProgramType.EMR_MR.name(), codeProgramType.getName())) {
            String cmd = buildCommand(parameter);
            return EmrCodeUtils.toEmrCode(codeProgramType, taskName, cmd);
        } else if (StringUtils.equalsIgnoreCase(CodeProgramType.ODPS_MR.name(), codeProgramType.getName())) {
            ResourceInfo mainJar = parameter.getMainJar();
            List<String> codeLines = new ArrayList<>();
            List<String> resources = new ArrayList<>();
            if (mainJar != null) {
                DolphinSchedulerV2Context context = DolphinSchedulerV2Context.getContext();
                String resourceName = CollectionUtils.emptyIfNull(context.getResources())
                        .stream()
                        .filter(r -> r.getId() == mainJar.getId())
                        .findAny()
                        .map(r -> r.getName())
                        .orElse("");
                resources.add(resourceName);
                codeLines.add(DataStudioCodeUtils.addResourceReference(codeProgramType, "", resources));
            }

            // convert to ODPS_MR
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
            code = EmrCodeUtils.toEmrCode(codeProgramType, taskName, code);
            EmrCode emrCode = EmrCodeUtils.asEmrCode(codeProgramType.getName(), code);
            Optional.ofNullable(emrCode).map(EmrCode::getLauncher)
                    .map(EmrLauncher::getAllocationSpec)
                    .map(EmrAllocationSpec::of)
                    .ifPresent(spec -> {
                        spec.setQueue(parameter.getQueue());
                        emrCode.getLauncher().setAllocationSpec(spec.toMap());
                    });
            return emrCode.getContent();
        } else {
            throw new RuntimeException("not support type " + codeProgramType.getName());
        }
    }

    protected String buildCommand(MapReduceParameters mapreduceParameters) {
        // hadoop jar <jar> [mainClass] [GENERIC_OPTIONS] args...
        List<String> args = new ArrayList<>();

        // other parameters
        args.addAll(buildArgs(mapreduceParameters));

        String command = ParameterUtils.convertParameterPlaceholders(String.join(" ", args),
                new HashMap<>());
        log.info("mapreduce task command: {}", command);

        return command;
    }

    public static List<String> buildArgs(MapReduceParameters param) {
        List<String> args = new ArrayList<>();

        ResourceInfo mainJar = param.getMainJar();
        if (mainJar != null) {
            DolphinSchedulerV2Context context = DolphinSchedulerV2Context.getContext();
            String resourceName = CollectionUtils.emptyIfNull(context.getResources())
                    .stream()
                    .filter(r -> r.getId() == mainJar.getId())
                    .findAny()
                    .map(r -> r.getName())
                    .orElse("");
            String resource = DataStudioCodeUtils.addResourceReference(CodeProgramType.EMR_MR, "", Arrays.asList(resourceName));
            args.add(resource + resourceName);
        }

        ProgramType programType = param.getProgramType();
        String mainClass = param.getMainClass();
        if (programType != null && programType != ProgramType.PYTHON && StringUtils.isNotEmpty(mainClass)) {
            args.add(mainClass);
        }

        String appName = param.getAppName();
        if (StringUtils.isNotEmpty(appName)) {
            args.add(String.format("%s%s=%s", D, MR_NAME, ArgsUtils.escape(appName)));
        }

        String others = param.getOthers();
        if (StringUtils.isEmpty(others) || !others.contains(MR_QUEUE)) {
            String queue = param.getQueue();
            if (StringUtils.isNotEmpty(queue)) {
                args.add(String.format("%s%s=%s", D, MR_QUEUE, queue));
            }
        }

        // -conf -archives -files -libjars -D
        if (StringUtils.isNotEmpty(others)) {
            args.add(others);
        }

        String mainArgs = param.getMainArgs();
        if (StringUtils.isNotEmpty(mainArgs)) {
            args.add(mainArgs);
        }
        return args;
    }

    private String getConverterType() {
        String convertType = properties.getProperty(Constants.CONVERTER_TARGET_MR_NODE_TYPE_AS);
        String defaultConvertType = CodeProgramType.EMR_MR.name();
        return getConverterType(convertType, defaultConvertType);
    }
}
