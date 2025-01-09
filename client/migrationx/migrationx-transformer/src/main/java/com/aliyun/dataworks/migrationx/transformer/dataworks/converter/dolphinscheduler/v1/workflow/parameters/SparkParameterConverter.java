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

import com.aliyun.dataworks.common.spec.domain.dw.codemodel.OdpsSparkCode;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.utils.ArgsUtils;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.DolphinSchedulerV1Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.ProcessMeta;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.TaskNode;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.entity.ResourceInfo;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.enums.ProgramType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.task.spark.SparkParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.spark.SparkConstants;
import com.aliyun.dataworks.migrationx.transformer.core.common.Constants;
import com.aliyun.dataworks.migrationx.transformer.core.utils.EmrCodeUtils;
import com.aliyun.migrationx.common.utils.BeanUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class SparkParameterConverter extends AbstractParameterConverter<SparkParameters> {

    public SparkParameterConverter(Properties properties, SpecWorkflow specWorkflow, ProcessMeta processMeta, TaskNode taskDefinition) {
        super(properties, specWorkflow, processMeta, taskDefinition);
    }

    /**
     * Each node translates the specific logic of the parameters
     */
    @Override
    protected void convertParameter(SpecNode specNode) {
        List<SpecVariable> specVariableList = convertSpecNodeParam(specNode);

        convertFileResourceList(specNode);

        String type = getConverterType();
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
        //String resourceReference = buildFileResourceReference(specNode, RESOURCE_REFERENCE_PREFIX);
        //script.setContent(resourceReference + parameter.getRawScript());
        script.setContent(convertCode(codeProgramType));
        script.setParameters(ListUtils.emptyIfNull(specVariableList).stream().filter(v -> !VariableType.NODE_OUTPUT.equals(v.getType()))
                .collect(Collectors.toList()));
        specNode.setScript(script);
    }

    public String convertCode(CodeProgramType codeProgramType) {
        DolphinSchedulerV1Context context = DolphinSchedulerV1Context.getContext();
        List<ResourceInfo> resourceInfos = context.getResources();

        Optional.ofNullable(parameter).map(SparkParameters::getMainJar)
                .flatMap(mainJar -> ListUtils.emptyIfNull(resourceInfos)
                        .stream().filter(res -> Objects.equals(res.getId(), mainJar.getId()))
                        .findFirst()).ifPresent(res -> parameter.setMainJar(res));

        ListUtils.emptyIfNull(Optional.ofNullable(parameter).map(SparkParameters::getResourceFilesList)
                        .orElse(ListUtils.emptyIfNull(null)))
                .forEach(res -> ListUtils.emptyIfNull(resourceInfos).stream()
                        .filter(res1 -> Objects.equals(res1.getId(), res.getId()))
                        .forEach(res1 -> {
                            BeanUtils.copyProperties(res1, res);
                            res.setRes(res1.getName());
                            res.setName(res1.getName());
                        }));

        if (CodeProgramType.EMR_SPARK.equals(codeProgramType)) {
            List<String> cmd = populateSparkOptions(parameter);
            String code = String.join(" ", cmd);
            return EmrCodeUtils.toEmrCode(codeProgramType, taskDefinition.getName(), code);
        } else if (CodeProgramType.ODPS_SPARK.equals(codeProgramType)) {
            OdpsSparkCode odpsSparkCode = populateSparkOdpsCode();
            return odpsSparkCode.toString();
        }
        throw new IllegalArgumentException("Unsupported code program type: " + codeProgramType);
    }

    private List<String> populateSparkOptions(SparkParameters sparkParameters) {
        List<String> args = new ArrayList<>();

        ProgramType programType = sparkParameters.getProgramType();
        ResourceInfo mainJar = sparkParameters.getMainJar();
        if (programType != ProgramType.SQL) {
            String resource = mainJar.getName();
            if (resource != null) {
                String[] resources = resource.split("/");
                if (resources.length > 0) {
                    resource = resources[resources.length - 1];
                }
            } else {
                DolphinSchedulerV1Context context = DolphinSchedulerV1Context.getContext();
                resource = CollectionUtils.emptyIfNull(context.getResources())
                        .stream()
                        .filter(r -> r.getId() == mainJar.getId())
                        .findAny()
                        .map(r -> r.getName())
                        .orElse(null);
                mainJar.setName(resource);
            }
            String dwResource = "##@resource_reference{\"" + resource + "\"} \n";
            args.add(dwResource + SparkConstants.SPARK_SUBMIT_COMMAND);
        } else {
            args.add(SparkConstants.SPARK_SUBMIT_COMMAND);
        }

        String deployMode = StringUtils.isNotEmpty(sparkParameters.getDeployMode()) ? sparkParameters.getDeployMode()
                : SparkConstants.DEPLOY_MODE_LOCAL;

        if (!SparkConstants.DEPLOY_MODE_LOCAL.equals(deployMode)) {
            args.add(SparkConstants.MASTER);
            String masterUrl = SparkConstants.SPARK_ON_YARN;
            args.add(masterUrl);
        }
        args.add(SparkConstants.DEPLOY_MODE);
        args.add(deployMode);

        String mainClass = sparkParameters.getMainClass();
        if (programType != ProgramType.PYTHON && programType != ProgramType.SQL && StringUtils.isNotEmpty(mainClass)) {
            args.add(SparkConstants.MAIN_CLASS);
            args.add(mainClass);
        }

        populateSparkResourceDefinitions(args, sparkParameters);

        String appName = sparkParameters.getAppName();
        if (StringUtils.isNotEmpty(appName)) {
            args.add(SparkConstants.SPARK_NAME);
            args.add(ArgsUtils.escape(appName));
        }

        String others = sparkParameters.getOthers();

        // --conf --files --jars --packages
        if (StringUtils.isNotEmpty(others)) {
            args.add(others);
        }

        //jar
        if (programType != ProgramType.SQL && mainJar != null) {
            args.add(mainJar.getName());
        }

        String mainArgs = sparkParameters.getMainArgs();
        if (programType != ProgramType.SQL && StringUtils.isNotEmpty(mainArgs)) {
            args.add(mainArgs);
        }

        return args;
    }

    private OdpsSparkCode populateSparkOdpsCode() {
        OdpsSparkCode odpsSparkCode = new OdpsSparkCode();
        odpsSparkCode.setResourceReferences(new ArrayList<>());
        odpsSparkCode.setSparkJson(new OdpsSparkCode.CodeJson());

        ResourceInfo mainJar = parameter.getMainJar();
        String resource = mainJar.getName();
        if (StringUtils.isEmpty(resource)) {
            resource = getResourceName(mainJar.getId());
        }

        if (resource != null) {
            String[] resources = resource.split("/");
            if (resources.length > 0) {
                resource = resources[resources.length - 1];
            }
            mainJar.setName(resource);
            //String dwResource = "##@resource_reference{\"" + resource + "\"} \n";
            odpsSparkCode.getResourceReferences().add(resource);
            odpsSparkCode.getSparkJson().setMainJar(resource);
        }
        String mainClass = parameter.getMainClass();
        odpsSparkCode.getSparkJson().setMainClass(mainClass);
        odpsSparkCode.getSparkJson().setVersion("2.x");
        odpsSparkCode.getSparkJson().setLanguage("java");

        String mainArgs = parameter.getMainArgs();
        odpsSparkCode.getSparkJson().setArgs(mainArgs);
        List<String> confs = new ArrayList<>();
        populateOdpsSparkResourceDefinitions(confs, parameter);
        odpsSparkCode.getSparkJson().setConfigs(confs);
        return odpsSparkCode;
    }

    private void populateOdpsSparkResourceDefinitions(List<String> args, SparkParameters sparkParameters) {
        int driverCores = sparkParameters.getDriverCores();
        if (driverCores > 0) {
            args.add(String.format("spark.driver.cores=%d", driverCores));
        }

        String driverMemory = sparkParameters.getDriverMemory();
        if (StringUtils.isNotEmpty(driverMemory)) {
            args.add(String.format("spark.driver.memory=%s", driverMemory));
        }

        int numExecutors = sparkParameters.getNumExecutors();
        if (numExecutors > 0) {
            args.add(String.format("spark.executor.instances=%d", numExecutors));
        }

        int executorCores = sparkParameters.getExecutorCores();
        if (executorCores > 0) {
            args.add(String.format("spark.executor.cores=%d", executorCores));
        }

        String executorMemory = sparkParameters.getExecutorMemory();
        if (StringUtils.isNotEmpty(executorMemory)) {
            args.add(String.format("spark.executor.memory=%s", executorMemory));
        }
    }

    private void populateSparkResourceDefinitions(List<String> args, SparkParameters sparkParameters) {
        int driverCores = sparkParameters.getDriverCores();
        if (driverCores > 0) {
            args.add(String.format(SparkConstants.DRIVER_CORES, driverCores));
        }

        String driverMemory = sparkParameters.getDriverMemory();
        if (StringUtils.isNotEmpty(driverMemory)) {
            args.add(String.format(SparkConstants.DRIVER_MEMORY, driverMemory));
        }

        int numExecutors = sparkParameters.getNumExecutors();
        if (numExecutors > 0) {
            args.add(String.format(SparkConstants.NUM_EXECUTORS, numExecutors));
        }

        int executorCores = sparkParameters.getExecutorCores();
        if (executorCores > 0) {
            args.add(String.format(SparkConstants.EXECUTOR_CORES, executorCores));
        }

        String executorMemory = sparkParameters.getExecutorMemory();
        if (StringUtils.isNotEmpty(executorMemory)) {
            args.add(String.format(SparkConstants.EXECUTOR_MEMORY, executorMemory));
        }
    }

    private String getResourceName(Integer id) {
        if (id == null) {
            return null;
        }
        DolphinSchedulerV1Context context = DolphinSchedulerV1Context.getContext();
        return CollectionUtils.emptyIfNull(context.getResources())
                .stream()
                .filter(r -> r.getId() == id)
                .findAny()
                .map(r -> r.getName())
                .orElse(null);
    }

    private String getConverterType() {
        String convertType = properties.getProperty(Constants.CONVERTER_TARGET_SPARK_SUBMIT_TYPE_AS);
        String defaultConvertType = CodeProgramType.EMR_SPARK_SHELL.name();
        return getConverterType(convertType, defaultConvertType);
    }
}
