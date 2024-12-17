/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.common;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.SpecRefEntity;
import com.aliyun.dataworks.common.spec.domain.dw.codemodel.AbstractBaseCode;
import com.aliyun.dataworks.common.spec.domain.dw.codemodel.ControllerJoinCode;
import com.aliyun.dataworks.common.spec.domain.dw.codemodel.ControllerJoinCode.Branch;
import com.aliyun.dataworks.common.spec.domain.dw.codemodel.ControllerJoinCode.Logic;
import com.aliyun.dataworks.common.spec.domain.dw.codemodel.ControllerJoinCode.Status;
import com.aliyun.dataworks.common.spec.domain.enums.DependencyType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableScopeType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableType;
import com.aliyun.dataworks.common.spec.domain.interfaces.LabelEnum;
import com.aliyun.dataworks.common.spec.domain.interfaces.Output;
import com.aliyun.dataworks.common.spec.domain.noref.SpecAssertIn;
import com.aliyun.dataworks.common.spec.domain.noref.SpecAssertion;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecJoin;
import com.aliyun.dataworks.common.spec.domain.noref.SpecJoinBranch;
import com.aliyun.dataworks.common.spec.domain.noref.SpecLogic;
import com.aliyun.dataworks.common.spec.domain.ref.SpecFileResource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScheduleStrategy;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecTrigger;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.exception.SpecErrorCode;
import com.aliyun.dataworks.common.spec.exception.SpecException;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.TaskType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
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
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.v320.task.condition.ConditionsParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.v320.task.dependent.DependentParameters;
import com.aliyun.dataworks.migrationx.transformer.core.utils.SpecFileResourceTypeUtils;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.common.context.DolphinSchedulerV3ConverterContext;
import com.aliyun.dataworks.migrationx.transformer.flowspec.model.SpecRefEntityWrapper;
import com.aliyun.migrationx.common.exception.BizException;
import com.aliyun.migrationx.common.exception.ErrorCode;
import com.aliyun.migrationx.common.utils.BeanUtils;
import com.aliyun.migrationx.common.utils.GsonUtils;
import com.google.gson.reflect.TypeToken;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-06-04
 */
@Slf4j
public abstract class AbstractParameterConverter<T extends AbstractParameters> extends AbstractCommonConverter<SpecNode> {

    private static final String RESOURCE_REFERENCE_FORMAT = "%s@resource_reference{\"%s\"}";

    protected final TaskDefinition taskDefinition;

    protected static Map<TaskType, Class<? extends AbstractParameters>> taskTypeClassMap;

    protected T parameter;

    @Getter
    protected final List<SpecRefEntityWrapper> headList = new ArrayList<>();

    @Getter
    protected final List<SpecRefEntityWrapper> tailList = new ArrayList<>();

    /**
     * The purpose of setting this field private is to mask the differences in how subclasses perceive spec and workflow
     */
    private final DataWorksWorkflowSpec spec;

    /**
     * The purpose of setting this field private is to mask the differences in how subclasses perceive spec and workflow
     */
    private final SpecWorkflow specWorkflow;

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
    }

    protected AbstractParameterConverter(DataWorksWorkflowSpec spec,
                                         SpecWorkflow specWorkflow, TaskDefinition taskDefinition, DolphinSchedulerV3ConverterContext context) {
        super(context);
        this.spec = spec;
        this.specWorkflow = specWorkflow;
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
        getWorkflowNodeList().add(specNode);

        // identify the beginning and end of the converted spec nodes
        if (CollectionUtils.isEmpty(headList)) {
            headList.add(newWrapper(specNode));
        }
        if (CollectionUtils.isEmpty(tailList)) {
            tailList.add(newWrapper(specNode));
        }

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
        specNode.setId(generateUuid(taskDefinition.getCode(), specNode));
        specNode.setName(taskDefinition.getName());
        specNode.setDescription(taskDefinition.getDescription());
        specNode.setRerunTimes(taskDefinition.getFailRetryTimes());
        // Unit conversion, minutes to milliseconds
        specNode.setRerunInterval((int)Duration.ofMinutes(taskDefinition.getFailRetryInterval()).toMillis());
        specNode.setTimeout(taskDefinition.getTimeout());
        specNode.setPriority(convertPriority(taskDefinition.getTaskPriority()));
        resetNodeStrategy(specNode);

        SpecNodeOutput defaultOutput = buildDefaultNodeOutput(specNode);
        /*
          hint: There is a significant problem here. Dozer is used for deep copying here. When copying an Output type, the source
          object may be a Variable or a NodeOutput, while the target object is an Output. In this case, if a NodeOutput object has not been
          copied previously, Dozer will initialize the target object as an Output type. However, since Output is an interface and lacks a
          constructor, this initialization will fail
          The solution is to first copy a NodeOutput object so that there exists a mapping for this type in Dozer's map. This will enable
          deep copying to proceed normally.
         */
        specNode.getOutputs().add(BeanUtils.deepCopy(defaultOutput, SpecNodeOutput.class));

        TriggerConverter triggerConverter = new TriggerConverter(getWorkflowTrigger(), taskDefinition, context);
        specNode.setTrigger(triggerConverter.convert());

        return specNode;
    }

    protected SpecNodeOutput buildDefaultNodeOutput(SpecNode specNode) {
        SpecNodeOutput specNodeOutput = new SpecNodeOutput();
        specNodeOutput.setIsDefault(true);
        specNodeOutput.setId(generateUuid());
        specNodeOutput.setData(specNode.getId());
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
                output -> output instanceof SpecNodeOutput && ((SpecNodeOutput)output).getIsDefault())
            .findFirst();
        if (throwException && !first.isPresent()) {
            throw new BizException(ErrorCode.PARAMETER_NOT_SET, "defaultOutput");
        }
        return (SpecNodeOutput)first.orElse(null);
    }

    protected List<SpecVariable> getContextOutputs(SpecNode specNode) {
        return specNode.getOutputs().stream()
            .filter(v -> v instanceof SpecVariable && VariableScopeType.NODE_CONTEXT.equals(((SpecVariable)v).getScope())
                && VariableType.NODE_OUTPUT.equals(((SpecVariable)v).getType()))
            .map(v -> {
                SpecVariable variable = BeanUtils.deepCopy(v, SpecVariable.class);
                variable.setNode(new SpecDepend(specNode, DependencyType.NORMAL, null));
                return variable;
            }).collect(Collectors.toList());
    }

    protected List<SpecVariable> convertSpecNodeParam(SpecNode specNode) {
        ParamListConverter paramListConverter = new ParamListConverter(taskDefinition.getTaskParamList(), taskDefinition, context);
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
            specFileResource.setName(getFileNameByPath(resourceInfo.getResourceName()));
            specFileResource.setType(SpecFileResourceTypeUtils.getResourceTypeBySuffix(specFileResource.getName()));
            checkFileSameName(specFileResource.getName(), resourceInfo.getResourceName());
            specNode.getFileResources().add(specFileResource);
        });
    }

    private void checkFileSameName(String fileName, String fullName) {
        String fullNameIn = context.getFileNameMap().get(fileName);
        if (Objects.nonNull(fullNameIn) && !fullNameIn.equals(fullName)) {
            log.warn("存在同名资源冲突风险, {} 和 {} 导入后会同名", fullNameIn, fullName);
        } else {
            context.getFileNameMap().put(fileName, fullName);
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

    protected SpecJoinBranch buildSpecJoinBranch(SpecNode specNode, ControllerJoinCode.Status assertion) {
        SpecJoinBranch specJoinBranch = new SpecJoinBranch();
        specJoinBranch.setNodeId(specNode);
        specJoinBranch.setName("b_" + specNode.getId());
        specJoinBranch.setOutput(getDefaultOutput(specNode, false));
        specJoinBranch.setAssertion(newAssertion(Collections.singletonList(assertion)));
        return specJoinBranch;
    }

    protected SpecJoinBranch buildSpecJoinBranch(SpecNodeOutput specNodeOutput, ControllerJoinCode.Status assertion) {
        SpecJoinBranch specJoinBranch = new SpecJoinBranch();
        specJoinBranch.setName("b_" + specNodeOutput.getId());
        specJoinBranch.setOutput(specNodeOutput);
        specJoinBranch.setAssertion(newAssertion(Collections.singletonList(assertion)));
        return specJoinBranch;
    }

    private SpecAssertion newAssertion(List<ControllerJoinCode.Status> statusList) {
        if (CollectionUtils.isEmpty(statusList)) {
            return null;
        }
        SpecAssertIn specAssertIn = new SpecAssertIn();
        specAssertIn.setValue(new ArrayList<>());
        ListUtils.emptyIfNull(statusList).stream().map(Status::getCode).forEach(code -> specAssertIn.getValue().add(code));

        SpecAssertion specAssertion = new SpecAssertion();
        specAssertion.setField("status");
        specAssertion.setIn(specAssertIn);
        return specAssertion;
    }

    /**
     * copy node, only used in dependent join.
     *
     * @param specNode origin node
     * @param suffix   new suffix
     * @return copied node
     */
    protected SpecNode copyJoinNode(SpecNode specNode, SpecJoin specJoin, String suffix) {
        SpecNode specNodeCopy = BeanUtils.deepCopy(specNode, SpecNode.class);
        specNodeCopy.setId(generateUuid(specNodeCopy));
        specNodeCopy.setName(specNodeCopy.getName() + suffix);
        for (Output output : specNodeCopy.getOutputs()) {
            if (output instanceof SpecNodeOutput && Boolean.TRUE.equals(((SpecNodeOutput)output).getIsDefault())) {
                ((SpecNodeOutput)output).setId(generateUuid());
                ((SpecNodeOutput)output).setData(specNodeCopy.getId());
                ((SpecNodeOutput)output).setRefTableName(specNodeCopy.getName());
            } else if (output instanceof SpecRefEntity) {
                ((SpecRefEntity)output).setId(generateUuid());
            }
        }
        getWorkflowNodeList().add(specNodeCopy);

        SpecScript scriptCopy = BeanUtils.deepCopy(specNodeCopy.getScript(), SpecScript.class);
        scriptCopy.setId(generateUuid());
        scriptCopy.setPath(scriptCopy.getPath() + suffix);

        specNodeCopy.setScript(scriptCopy);

        specNodeCopy.setJoin(specJoin);

        // build script content
        Optional.ofNullable(buildControllerJoinCode(specNodeCopy))
            .map(AbstractBaseCode::getContent)
            .ifPresent(scriptCopy::setContent);
        return specNodeCopy;
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
        String defaultPath = StringUtils.defaultString(context.getDefaultScriptPath(), StringUtils.EMPTY);
        String workFlowPath = Optional.ofNullable(specWorkflow)
            .map(SpecWorkflow::getName)
            .orElse(Optional.ofNullable(spec)
                .map(DataWorksWorkflowSpec::getName).orElse(""));
        return FilenameUtils.concat(FilenameUtils.concat(defaultPath, workFlowPath), specNode.getName());
    }

    protected String getScriptPath(String name) {
        if (StringUtils.isBlank(name)) {
            return StringUtils.EMPTY;
        }
        String defaultPath = StringUtils.defaultString(context.getDefaultScriptPath(), StringUtils.EMPTY);
        String workFlowPath = Optional.ofNullable(specWorkflow)
            .map(SpecWorkflow::getName)
            .orElse(Optional.ofNullable(spec)
                .map(DataWorksWorkflowSpec::getName).orElse(""));
        return FilenameUtils.concat(FilenameUtils.concat(defaultPath, workFlowPath), name);
    }

    protected List<SpecNode> getWorkflowNodeList() {
        if (Objects.nonNull(specWorkflow)) {
            return specWorkflow.getNodes();
        }
        return Optional.ofNullable(spec).map(DataWorksWorkflowSpec::getNodes).orElseThrow(
            () -> new BizException(ErrorCode.PARAMETER_NOT_SET, "spec or specWorkflow"));
    }

    protected List<SpecFlowDepend> getWorkflowDependencyList() {
        if (Objects.nonNull(specWorkflow)) {
            return specWorkflow.getDependencies();
        }
        return Optional.ofNullable(spec).map(DataWorksWorkflowSpec::getFlow).orElseThrow(
            () -> new BizException(ErrorCode.PARAMETER_NOT_SET, "spec or specWorkflow"));
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

    protected ControllerJoinCode buildControllerJoinCode(SpecNode specNode) {
        SpecJoin join = specNode.getJoin();
        if (join == null) {
            return null;
        }
        ControllerJoinCode code = new ControllerJoinCode();
        code.setResultStatus(join.getResultStatus());
        code.setBranchList(new ArrayList<>());

        Map<String, SpecJoinBranch> branchMap = ListUtils.emptyIfNull(join.getBranches()).stream()
            .collect(Collectors.toMap(SpecJoinBranch::getName, Function.identity()));

        // make express like [logic branchName], e.g. [and b_1 or b_2 and b_3]
        String express = Optional.ofNullable(join.getLogic()).map(SpecLogic::getExpression)
            .map(s -> "and " + s).orElse("");
        log.info("express: {}", express);
        String[] tokens = express.split("\\s+");
        if (tokens.length % 2 != 0) {
            throw new SpecException(SpecErrorCode.PARSE_ERROR, "node.join.logic.expression is invalid");
        }
        for (int i = 0; i < tokens.length; i += 2) {
            Logic logic = LabelEnum.getByLabel(Logic.class, tokens[i]);
            if (logic == null) {
                throw new SpecException(SpecErrorCode.PARSE_ERROR, "node.join.logic.expression is invalid");
            }
            String branchName = tokens[i + 1];
            SpecJoinBranch branch = branchMap.get(branchName);
            if (branch == null) {
                throw new SpecException(SpecErrorCode.PARSE_ERROR, "node.join.logic.expression is invalid");
            }
            Branch b = new Branch();
            String nodeId = getNodeIdFromSpecBranch(branch);
            b.setNode(nodeId);
            b.setNodeUuid(nodeId);
            b.setNodeName(getNodeNameFromSpecBranch(branch));
            b.setLogic(logic.getCode());
            b.setRunStatus(Optional.ofNullable(branch.getAssertion())
                .map(SpecAssertion::getIn)
                .map(SpecAssertIn::getValue)
                .orElse(Collections.emptyList()).stream()
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .collect(Collectors.toList()));
            code.getBranchList().add(b);
        }
        return code;
    }

    private String getNodeIdFromSpecBranch(SpecJoinBranch branch) {
        return Optional.ofNullable(branch)
            .map(SpecJoinBranch::getNodeId)
            .map(SpecRefEntity::getId)
            .orElseGet(() -> Optional.ofNullable(branch)
                .map(SpecJoinBranch::getOutput)
                .filter(output -> BooleanUtils.isTrue(output.getIsDefault()))
                .map(SpecNodeOutput::getData).orElse(null));
    }

    private String getNodeNameFromSpecBranch(SpecJoinBranch branch) {
        return Optional.ofNullable(branch)
            .map(SpecJoinBranch::getNodeId)
            .map(SpecNode::getName)
            .orElseGet(() -> Optional.ofNullable(branch)
                .map(SpecJoinBranch::getOutput)
                .filter(output -> BooleanUtils.isTrue(output.getIsDefault()))
                .map(SpecNodeOutput::getRefTableName).orElse(null));
    }
}
