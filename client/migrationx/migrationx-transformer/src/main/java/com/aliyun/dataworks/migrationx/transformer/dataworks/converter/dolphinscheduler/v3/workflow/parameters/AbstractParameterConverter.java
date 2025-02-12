/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.dw.types.LanguageEnum;
import com.aliyun.dataworks.common.spec.domain.enums.DependencyType;
import com.aliyun.dataworks.common.spec.domain.enums.NodeRecurrenceType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableScopeType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableType;
import com.aliyun.dataworks.common.spec.domain.interfaces.Output;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.ref.SpecFileResource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScheduleStrategy;
import com.aliyun.dataworks.common.spec.domain.ref.SpecTrigger;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.TaskType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DolphinSchedulerV3Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.ProcessDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.Flag;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.Priority;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.datax.DataxParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.flink.FlinkParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.http.HttpParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.mr.MapReduceParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.parameters.AbstractParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.procedure.ProcedureParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.python.PythonParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.shell.ShellParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.spark.SparkParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.sql.SqlParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.sqoop.SqoopParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.subprocess.SubProcessParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.switchs.SwitchParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.v320.task.condition.ConditionsParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.v320.task.dependent.DependentParameters;
import com.aliyun.dataworks.migrationx.transformer.core.utils.SpecFileResourceTypeUtils;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.utils.ConverterTypeUtils;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.ParamListConverter;
import com.aliyun.migrationx.common.exception.BizException;
import com.aliyun.migrationx.common.exception.ErrorCode;
import com.aliyun.migrationx.common.utils.BeanUtils;
import com.aliyun.migrationx.common.utils.Config;
import com.aliyun.migrationx.common.utils.GsonUtils;
import com.aliyun.migrationx.common.utils.UuidGenerators;

import com.google.gson.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public abstract class AbstractParameterConverter<T extends AbstractParameters> {

    protected static final String RESOURCE_REFERENCE_FORMAT = "%s@resource_reference{\"%s\"}";
    protected static final String RESOURCE_REFERENCE_PREFIX = "##";

    protected final TaskDefinition taskDefinition;
    protected final DagData processMeta;
    protected final ProcessDefinition processDefinition;
    protected final Properties properties;

    protected static Map<TaskType, Class<? extends AbstractParameters>> taskTypeClassMap;

    protected T parameter;

    /**
     * The purpose of setting this field private is to mask the differences in how subclasses perceive spec and workflow
     */
    protected final SpecWorkflow specWorkflow;

    static {
        taskTypeClassMap = new EnumMap<>(TaskType.class);
        taskTypeClassMap.put(TaskType.SQL, SqlParameters.class);
        taskTypeClassMap.put(TaskType.DEPENDENT, DependentParameters.class);
        taskTypeClassMap.put(TaskType.FLINK, FlinkParameters.class);
        taskTypeClassMap.put(TaskType.SPARK, SparkParameters.class);
        taskTypeClassMap.put(TaskType.DATAX, DataxParameters.class);
        taskTypeClassMap.put(TaskType.SHELL, ShellParameters.class);
        taskTypeClassMap.put(TaskType.HTTP, HttpParameters.class);
        taskTypeClassMap.put(TaskType.PROCEDURE, ProcedureParameters.class);
        taskTypeClassMap.put(TaskType.CONDITIONS, ConditionsParameters.class);
        taskTypeClassMap.put(TaskType.SQOOP, SqoopParameters.class);
        taskTypeClassMap.put(TaskType.SUB_PROCESS, SubProcessParameters.class);
        taskTypeClassMap.put(TaskType.PYTHON, PythonParameters.class);
        taskTypeClassMap.put(TaskType.MR, MapReduceParameters.class);
        taskTypeClassMap.put(TaskType.SWITCH, SwitchParameters.class);
    }

    protected AbstractParameterConverter(Properties properties,
            SpecWorkflow specWorkflow, DagData processMeta, TaskDefinition taskDefinition) {
        super();
        this.properties = properties;
        //this.spec = spec;
        this.specWorkflow = specWorkflow;
        this.processMeta = processMeta;
        this.processDefinition = processMeta.getProcessDefinition();
        this.taskDefinition = taskDefinition;

        TaskType taskType = TaskType.valueOf(taskDefinition.getTaskType());
        try {
            this.parameter = GsonUtils.fromJsonString(
                    taskDefinition.getTaskParams(), TypeToken.get(taskTypeClassMap.get(taskType)).getType());
        } catch (Exception ex) {
            log.error("parse task {}, parameter {} error: ", taskType, taskTypeClassMap.get(taskType), ex);
        }
    }

    /**
     * Each node translates the specific logic of the parameters
     */
    protected abstract void convertParameter(SpecNode specNode);

    public SpecNode convert() {
        SpecNode specNode = newSpecNode(taskDefinition);
        convertParameter(specNode);
        this.specWorkflow.getNodes().add(specNode);

        // hint: the node returned may not be the final result of the conversion
        return specNode;
    }

    private SpecNode initSpecNode() {
        SpecNode specNode = new SpecNode();
        specNode.setInputs(new ArrayList<>());
        specNode.setOutputs(new ArrayList<>());
        specNode.setFileResources(new ArrayList<>());
        specNode.setFunctions(new ArrayList<>());
        return specNode;
    }

    /**
     * common new SpecNode method, almost all nodes use this method
     *
     * @param taskDefinition taskDefinition
     * @return SpecNode
     */
    protected SpecNode newSpecNode(TaskDefinition taskDefinition) {
        SpecNode specNode = initSpecNode();
        specNode.setId(UuidGenerators.generateUuid(taskDefinition.getCode()));
        specNode.setName(taskDefinition.getName());
        specNode.setDescription(taskDefinition.getDescription());
        specNode.setRerunTimes(taskDefinition.getFailRetryTimes());
        // Unit conversion, minutes to milliseconds
        specNode.setRerunInterval((int) Duration.ofMinutes(taskDefinition.getFailRetryInterval()).toMillis());
        specNode.setTimeout(taskDefinition.getTimeout());
        specNode.setPriority(convertPriority(taskDefinition.getTaskPriority()));
        resetNodeStrategy(specNode);

        SpecNodeOutput defaultOutput = buildDefaultNodeOutput(specNode);
        DolphinSchedulerV3Context.getContext().getTaskCodeNodeDataMap().put(taskDefinition.getCode(), defaultOutput.getData());
        DolphinSchedulerV3Context.getContext().getTaskCodeNodeIdMap().put(taskDefinition.getCode(), specNode.getId());
        //specNode.getOutputs().add(BeanUtils.deepCopy(defaultOutput, SpecNodeOutput.class));

        specNode.getOutputs().add(defaultOutput);

        specNode.setTrigger(convertByTaskDefinition());

        return specNode;
    }

    private SpecTrigger convertByTaskDefinition() {
        SpecTrigger specTrigger = new SpecTrigger();
        specTrigger.setDelaySeconds((int) Duration.ofMinutes(taskDefinition.getDelayTime()).getSeconds());
        specTrigger.setRecurrence(Flag.YES.equals(taskDefinition.getFlag()) ? NodeRecurrenceType.NORMAL : NodeRecurrenceType.PAUSE);
        specTrigger.setId(UuidGenerators.generateUuid());
        return specTrigger;
    }

    protected Integer convertPriority(Priority priority) {
        return Priority.LOWEST.getCode() - priority.getCode();
    }

    protected SpecNodeOutput buildDefaultNodeOutput(SpecNode specNode) {
        SpecNodeOutput specNodeOutput = new SpecNodeOutput();
        specNodeOutput.setIsDefault(true);
        //specNodeOutput.setId(generateUuid());
        //String data = String.format("%s.%s", processDefinition.getCode(), taskDefinition.getCode());
        String data = String.format("%s.%s.%s", processDefinition.getProjectName(), processDefinition.getName(), taskDefinition.getName());
        specNodeOutput.setData(data);
        specNodeOutput.setRefTableName(specNode.getName());
        return specNodeOutput;
    }

    protected SpecNodeOutput getDefaultOutput(SpecNode specNode) {
        return getDefaultOutput(Optional.ofNullable(specNode).map(SpecNode::getOutputs).orElse(null));
    }

    protected SpecNodeOutput getDefaultOutput(SpecWorkflow specWorkflow, boolean throwException) {
        return getDefaultOutput(Optional.ofNullable(specWorkflow).map(SpecWorkflow::getOutputs).orElse(null), throwException);
    }

    protected SpecNodeOutput getDefaultOutput(SpecNode specNode, boolean throwException) {
        return getDefaultOutput(Optional.ofNullable(specNode).map(SpecNode::getOutputs).orElse(null), throwException);
    }

    protected SpecNodeOutput getDefaultOutput(List<Output> outputList) {
        return getDefaultOutput(outputList, false);
    }

    protected SpecNodeOutput getDefaultOutput(List<Output> outputList, boolean throwException) {
        Optional<Output> first = ListUtils.emptyIfNull(outputList).stream().filter(
                        output -> output instanceof SpecNodeOutput && ((SpecNodeOutput) output).getIsDefault())
                .findFirst();
        if (throwException && !first.isPresent()) {
            throw new BizException(ErrorCode.PARAMETER_NOT_SET, "defaultOutput");
        }
        return (SpecNodeOutput) first.orElse(null);
    }

    protected List<SpecVariable> getContextOutputs(SpecNode specNode) {
        return specNode.getOutputs().stream()
                .filter(v -> v instanceof SpecVariable && VariableScopeType.NODE_CONTEXT.equals(((SpecVariable) v).getScope())
                        && VariableType.NODE_OUTPUT.equals(((SpecVariable) v).getType()))
                .map(v -> {
                    SpecVariable variable = BeanUtils.deepCopy(v, SpecVariable.class);
                    variable.setNode(new SpecDepend(specNode, DependencyType.NORMAL, null));
                    return variable;
                }).collect(Collectors.toList());
    }

    protected List<SpecVariable> convertSpecNodeParam(SpecNode specNode) {
        ParamListConverter paramListConverter = new ParamListConverter(taskDefinition.getTaskParamList(), taskDefinition);
        List<SpecVariable> specVariableList = paramListConverter.convert();
        for (SpecVariable specVariable : specVariableList) {
            // all outputs are context output, all inputs are all script inputs
            if (VariableType.NODE_OUTPUT.equals(specVariable.getType())) {
                SpecDepend nodeDepend = new SpecDepend();
                nodeDepend.setNodeId(specNode);
                nodeDepend.setOutput(getDefaultOutput(specNode));
                specVariable.setNode(nodeDepend);
                specNode.getOutputs().add(specVariable);
            }
        }
        return specVariableList;
    }

    /**
     * convert resource with fileResources info in workflow, if not exists in fileResources, create and add in fileResources
     *
     * @param specNode node need to convert
     */
    protected void convertFileResourceList(SpecNode specNode) {
        ListUtils.emptyIfNull(parameter.getResourceFilesList()).forEach(resourceInfo -> {
            SpecFileResource specFileResource = new SpecFileResource();
            specFileResource.setRuntimeResource(specNode.getRuntimeResource());
            if (resourceInfo.getResourceName() != null) {
                specFileResource.setName(getFileNameByPath(resourceInfo.getResourceName()));
            }

            specFileResource.setType(SpecFileResourceTypeUtils.getResourceTypeBySuffix(specFileResource.getName()));
            checkFileSameName(specFileResource.getName(), resourceInfo.getResourceName());
            specNode.getFileResources().add(specFileResource);
        });
    }

    private void checkFileSameName(String fileName, String fullName) {
        //String fullNameIn = context.getFileNameMap().get(fileName);
        String fullNameIn = null;
        if (Objects.nonNull(fullNameIn) && !fullNameIn.equals(fullName)) {
            log.warn("存在同名资源冲突风险, {} 和 {} 导入后会同名", fullNameIn, fullName);
        } else {
            //context.getFileNameMap().put(fileName, fullName);
        }
    }

    protected String buildFileResourceReference(SpecNode specNode, String prefix) {
        StringBuilder stringBuilder = new StringBuilder();
        Optional.ofNullable(specNode).map(SpecNode::getFileResources)
                .ifPresent(fileResources ->
                        fileResources.forEach(fileResource ->
                                stringBuilder.append(String.format(RESOURCE_REFERENCE_FORMAT, prefix, fileResource.getName())).append("\n")));
        return stringBuilder.append("\n").toString();
    }

    /**
     * add relation before join node. if the node depend on a whole workflow, need depend on workflow output
     *
     * @param postNode          post join node
     * @param preNodeList       pre node list
     * @param preNodeOutputList pre workflow output list
     */
    protected void addRelation(SpecNode postNode, List<SpecNode> preNodeList, List<SpecNodeOutput> preNodeOutputList) {
        SpecFlowDepend specFlowDepend = newSpecFlowDepend();
        specFlowDepend.setNodeId(postNode);
        ListUtils.emptyIfNull(preNodeList).forEach(preNode -> {
            SpecNodeOutput preNodeOutput = getDefaultOutput(preNode);
            postNode.getInputs().add(preNodeOutput);
            postNode.getInputs().addAll(getContextOutputs(preNode));
            specFlowDepend.getDepends().add(new SpecDepend(preNode, DependencyType.NORMAL, preNodeOutput));
        });

        ListUtils.emptyIfNull(preNodeOutputList).forEach(preNodeOutput -> {
            postNode.getInputs().add(preNodeOutput);
            SpecDepend specDepend = new SpecDepend();
            specDepend.setType(DependencyType.NORMAL);
            specDepend.setOutput(preNodeOutput);
            specFlowDepend.getDepends().add(specDepend);
        });
        getWorkflowDependencyList().add(specFlowDepend);
    }

    protected SpecFlowDepend newSpecFlowDepend() {
        SpecFlowDepend specFlowDepend = new SpecFlowDepend();
        specFlowDepend.setDepends(new ArrayList<>());
        return specFlowDepend;
    }

    protected void addRelation(SpecNode postNode, List<SpecNode> preNodeList) {
        addRelation(postNode, preNodeList, null);
    }

    protected String getFileNameByPath(String path) {
        File file = new File(path);
        return file.getName();
    }

    protected String getScriptPath(SpecNode specNode) {
        if (Objects.isNull(specNode)) {
            return StringUtils.EMPTY;
        }
        String defaultPath = StringUtils.defaultString(Config.get().getBasePath(), StringUtils.EMPTY);
        String workFlowPath = Optional.ofNullable(specWorkflow)
                .map(SpecWorkflow::getName)
                .orElse(StringUtils.EMPTY);
        return FilenameUtils.concat(FilenameUtils.concat(defaultPath, workFlowPath), specNode.getName());
    }

    protected List<SpecFlowDepend> getWorkflowDependencyList() {
        if (Objects.nonNull(specWorkflow)) {
            return specWorkflow.getDependencies();
        } else {
            return Collections.emptyList();
        }
    }

    protected SpecTrigger getWorkflowTrigger() {
        if (Objects.nonNull(specWorkflow)) {
            return specWorkflow.getTrigger();
        }
        // may be manual flow
        return null;
    }

    /**
     * get workflow, but it will be null in spec version < 1.2.0
     *
     * @return SpecWorkflow if it is not null
     */
    protected SpecWorkflow getWorkFlow() {
        return specWorkflow;
    }

    protected void resetNodeStrategy(SpecNode specNode) {
        if (specNode.getStrategy() == null) {
            specNode.setStrategy(new SpecScheduleStrategy());
        }
        SpecScheduleStrategy strategy = specNode.getStrategy();
        strategy.setPriority(specNode.getPriority());
        strategy.setTimeout(specNode.getTimeout());
        strategy.setRerunInterval(specNode.getRerunInterval());
        strategy.setRerunTimes(specNode.getRerunTimes());
        strategy.setIgnoreBranchConditionSkip(specNode.getIgnoreBranchConditionSkip());
        strategy.setInstanceMode(specNode.getInstanceMode());
        strategy.setRerunMode(specNode.getRerunMode());

        Optional.ofNullable(getWorkFlow())
                .map(SpecWorkflow::getStrategy)
                .map(SpecScheduleStrategy::getFailureStrategy)
                .ifPresent(strategy::setFailureStrategy);
    }

    protected String codeToLanguageIdentifier(CodeProgramType nodeType) {
        LanguageEnum languageEnum = codeToLanguage(nodeType);
        if (languageEnum == null) {
            log.warn("can not find language by {}", nodeType);
            return null;
        }
        return languageEnum.getIdentifier();
    }

    protected LanguageEnum codeToLanguage(CodeProgramType nodeType) {
        switch (nodeType) {
            case SHELL:
            case DIDE_SHELL:
            case CDH_SHELL:
            case EMR_SPARK_SHELL:
            case CDH_SPARK_SHELL:
            case EMR_SHELL:
            case EMR_HIVE_CLI:
            case PERL:
                return LanguageEnum.SHELL_SCRIPT;
            case EMR_SPARK_SQL:
                return LanguageEnum.SPARK_SQL;
            case CDH_HIVE:
            case HIVE:
            case EMR_HIVE:
                return LanguageEnum.HIVE_SQL;
            case EMR_IMPALA:
            case CDH_IMPALA:
                return LanguageEnum.IMPALA_SQL;
            case CLICK_SQL:
                return LanguageEnum.CLICKHOUSE_SQL;
            case ODPS_SQL:
            case ODPS_PERL:
                return LanguageEnum.ODPS_SQL;
            case ODPS_SCRIPT:
                return LanguageEnum.ODPS_SCRIPT;
            case EMR_PRESTO:
            case CDH_PRESTO:
                return LanguageEnum.PRESTO_SQL;
            case PYODPS:
                return LanguageEnum.PYTHON2;
            case PYODPS3:
            case PYTHON:
                return LanguageEnum.PYTHON3;
            case DATAX2:
            case DATAX:
            case RI:
            case DI:
                return LanguageEnum.JSON;
            case HOLOGRES_SQL:
                return LanguageEnum.HOLOGRES_SQL;
            default:
                return null;
        }
    }

    protected String getConverterType(String convertType, String defaultConvertType) {
        String projectName = processDefinition.getProjectName();
        String processName = processDefinition.getName();
        String taskName = taskDefinition.getName();
        return ConverterTypeUtils.getConverterType(convertType, projectName, processName, taskName, defaultConvertType);
    }
}
