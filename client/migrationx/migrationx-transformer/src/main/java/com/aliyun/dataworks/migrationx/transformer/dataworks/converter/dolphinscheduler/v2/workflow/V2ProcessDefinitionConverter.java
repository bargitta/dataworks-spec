/*
 * Copyright (c) 2024, Alibaba Cloud;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v2.workflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.dw.codemodel.ControllerBranchCode.Branch;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.FailureStrategy;
import com.aliyun.dataworks.common.spec.domain.enums.NodeInstanceModeType;
import com.aliyun.dataworks.common.spec.domain.enums.NodeRerunModeType;
import com.aliyun.dataworks.common.spec.domain.noref.SpecBranches;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScheduleStrategy;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecTrigger;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.DolphinSchedulerV2Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.ProcessDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.ProcessTaskRelation;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.Schedule;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.Priority;
import com.aliyun.dataworks.migrationx.transformer.core.checkpoint.CheckPoint;
import com.aliyun.dataworks.migrationx.transformer.core.checkpoint.StoreWriter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.filters.DolphinSchedulerConverterFilter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v2.workflow.parameters.AbstractParameterConverter;
import com.aliyun.migrationx.common.context.TransformerContext;
import com.aliyun.migrationx.common.exception.UnSupportedTypeException;
import com.aliyun.migrationx.common.metrics.DolphinMetrics;
import com.aliyun.migrationx.common.utils.Config;
import com.aliyun.migrationx.common.utils.UuidGenerators;

import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class V2ProcessDefinitionConverter {
    private List<SpecNode> specNodes = new ArrayList<>();

    private static final SpecScriptRuntime WORKFLOW_RUNTIME = new SpecScriptRuntime();

    private static final SpecScriptRuntime MANUAL_WORKFLOW_RUNTIME = new SpecScriptRuntime();

    static {
        WORKFLOW_RUNTIME.setEngine(CodeProgramType.VIRTUAL_WORKFLOW.getCalcEngineType().getLabel());
        WORKFLOW_RUNTIME.setCommand("WORKFLOW");

        MANUAL_WORKFLOW_RUNTIME.setEngine(CodeProgramType.VIRTUAL_WORKFLOW.getCalcEngineType().getLabel());
        MANUAL_WORKFLOW_RUNTIME.setCommand("MANUAL_WORKFLOW");
    }

    private ProcessDefinition processDefinition;
    private DagData dagData;
    private Properties converterProperties;
    private List<TaskDefinition> taskDefinitionList;

    private DolphinSchedulerConverterFilter filter;

    public V2ProcessDefinitionConverter(DagData dagData, Properties converterProperties) {
        this.dagData = dagData;
        this.converterProperties = converterProperties;
        this.processDefinition = dagData.getProcessDefinition();
        this.taskDefinitionList = dagData.getTaskDefinitionList();
        this.filter = new DolphinSchedulerConverterFilter();
    }

    public static String toWorkflowName(ProcessDefinition processDefinition) {
        if (processDefinition.getProjectName() == null) {
            return processDefinition.getName();
        }
        return com.aliyun.dataworks.migrationx.domain.dataworks.utils.StringUtils.toValidName(Joiner.on("_").join(
                processDefinition.getProjectName(), processDefinition.getName()));
    }

    private SpecWorkflow initWorkflow() {
        SpecWorkflow specWorkflow = new SpecWorkflow();
        specWorkflow.setDependencies(new ArrayList<>());
        specWorkflow.setNodes(new ArrayList<>());
        specWorkflow.setInputs(new ArrayList<>());
        specWorkflow.setOutputs(new ArrayList<>());
        return specWorkflow;
    }

    public SpecWorkflow convert() {
        SpecWorkflow specWorkflow = convertProcess(processDefinition);
        DolphinSchedulerV2Context.getContext().getSubProcessCodeWorkflowMap().put(processDefinition.getCode(), specWorkflow);
        convertTaskDefinitions(specWorkflow);
        convertTrigger(specWorkflow);
        convertTaskRelations(specWorkflow);
        handleBranch(specWorkflow);
        return specWorkflow;
    }

    protected SpecWorkflow convertProcess(ProcessDefinition processDefinition) {
        log.info("convert workflow,processDefinition: {}", processDefinition.getName());

        SpecWorkflow specWorkflow = initWorkflow();
        specWorkflow.setId(UuidGenerators.generateUuid(processDefinition.getCode()));
        specWorkflow.setName(toWorkflowName(processDefinition));
        specWorkflow.setDescription(processDefinition.getDescription());

        List<SpecVariable> specVariableList = new ParamListConverter(processDefinition.getGlobalParamList()).convert();
        log.info("convert workflow,global params: {}", specVariableList);

        SpecScript script = new SpecScript();
        script.setParameters(specVariableList);
        script.setRuntime(WORKFLOW_RUNTIME);
        script.setPath(getScriptPath(specWorkflow.getName()));
        specWorkflow.setScript(script);

        specWorkflow.getOutputs().add(buildDefaultOutput(specWorkflow));
        return specWorkflow;
    }

    protected String getScriptPath(String name) {
        String defaultPath = StringUtils.defaultString(Config.INSTANCE.getBasePath(), StringUtils.EMPTY);
        return FilenameUtils.concat(defaultPath, name);
    }

    protected List<SpecNode> convertTaskDefinitions(SpecWorkflow specWorkflow) {
        return convertTasks(specWorkflow);
    }

    protected void convertTrigger(SpecWorkflow specWorkflow) {
        Schedule schedule = dagData.getSchedule();
        if (Objects.nonNull(schedule)) {
            SpecTrigger trigger = new TriggerConverter(schedule).convert();
            specWorkflow.setTrigger(trigger);
            specWorkflow.setStrategy(buildSpecScheduleStrategy(processDefinition, schedule));
            log.info("convert workflow,schedule: {}", schedule);
        }
    }

    /**
     * when handling task, spec node maybe not exists, post handle branch
     * change branch output data (task code) to specNode id
     */
    private void handleBranch(SpecWorkflow specWorkflow) {
        Map<Long, Object> taskCodeSpecNodeMap = DolphinSchedulerV2Context.getContext().getTaskCodeSpecNodeMap();
        for (SpecNode specNode : specWorkflow.getNodes()) {
            if (specNode.getBranch() != null && specNode.getBranch().getBranches() != null) {
                List<SpecBranches> specBranches = specNode.getBranch().getBranches();
                List<Branch> branchList = new ArrayList<>();
                for (SpecBranches specBranch : specBranches) {
                    SpecNodeOutput specNodeOutput = specBranch.getOutput();
                    if (specNodeOutput != null && specNodeOutput.getData() != null) {
                        String data = specNodeOutput.getData();
                        //current data is task code, convert to specNode id
                        Long taskCode = Long.parseLong(data);
                        SpecNode branchSpecNode = (SpecNode) taskCodeSpecNodeMap.get(taskCode);
                        specNodeOutput.setData(branchSpecNode.getId());
                        Branch branch = new Branch();
                        branch.setCondition(specBranch.getWhen());
                        branch.setNodeoutput(branchSpecNode.getId());
                        branchList.add(branch);
                    }
                }
                if (branchList.size() > 0) {
                    String content = com.aliyun.dataworks.common.spec.utils.GsonUtils.toJsonString(branchList);
                    specNode.getScript().setContent(content);
                }
            }
        }
    }

    protected void convertTaskRelations(SpecWorkflow specWorkflow) {
        List<ProcessTaskRelation> processTaskRelationList = dagData.getProcessTaskRelationList();
        log.info("convert workflow,processTaskRelationList: {}", processTaskRelationList);
        new SpecFlowDependConverter(null, specWorkflow, processTaskRelationList).convert();
    }

    private SpecScheduleStrategy buildSpecScheduleStrategy(ProcessDefinition processDefinition, Schedule schedule) {
        SpecScheduleStrategy strategy = new SpecScheduleStrategy();
        strategy.setPriority(convertPriority(schedule.getProcessInstancePriority()));
        strategy.setTimeout(processDefinition.getTimeout());
        strategy.setInstanceMode(NodeInstanceModeType.T_PLUS_1);
        strategy.setRerunMode(NodeRerunModeType.ALL_ALLOWED);
        strategy.setRerunTimes(0);
        strategy.setRerunInterval(0);
        strategy.setIgnoreBranchConditionSkip(false);
        strategy.setFailureStrategy(
                com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.FailureStrategy.CONTINUE.equals(schedule.getFailureStrategy())
                        ? FailureStrategy.CONTINUE
                        : FailureStrategy.BREAK);
        return strategy;
    }

    protected Integer convertPriority(Priority priority) {
        return Priority.LOWEST.getCode() - priority.getCode();
    }

    private SpecNodeOutput buildDefaultOutput(SpecWorkflow specWorkflow) {
        SpecNodeOutput specNodeOutput = new SpecNodeOutput();
        specNodeOutput.setIsDefault(true);
        //String data = String.valueOf(processDefinition.getCode());
        String data = String.format("%s.%s", processDefinition.getProjectName(), processDefinition.getName());
        specNodeOutput.setData(data);
        specNodeOutput.setRefTableName(specWorkflow.getName());
        return specNodeOutput;
    }

    public List<SpecNode> convertTasks(SpecWorkflow specWorkflow) {
        CheckPoint checkPoint = CheckPoint.getInstance();
        String projectName = processDefinition.getProjectName();
        String processName = processDefinition.getName();

        Map<String, List<SpecNode>> loadedTasks = checkPoint.loadFromCheckPoint(projectName, processName);

        final Function<StoreWriter, List<SpecNode>> processFunc = (StoreWriter writer) ->
                ListUtils.emptyIfNull(taskDefinitionList)
                        .stream()
                        .filter(s -> {
                            if (Thread.currentThread().isInterrupted()) {
                                throw new RuntimeException(new InterruptedException());
                            }
                            return true;
                        })
                        .filter(task -> {
                            boolean passed = filter.filter(projectName, processName, task.getName());
                            if (!passed) {
                                log.warn("task filtered {}", task.getName());
                            }
                            return passed;
                        })
                        .map(task -> {
                            List<SpecNode> specNodes = convertTaskToWorkflowWithLoadedTask(specWorkflow, task, loadedTasks);
                            checkPoint.doCheckpoint(writer, specNodes, processName, task.getName());
                            return specNodes;
                        })
                        .flatMap(List::stream)
                        .collect(Collectors.toList());

        specNodes = checkPoint.doWithCheckpoint(processFunc, projectName);

        log.info("successfully converted {}, size {}", processDefinition.getName(), specNodes.size());
        return specNodes;
    }

    private List<SpecNode> convertTaskToWorkflowWithLoadedTask(SpecWorkflow specWorkflow, TaskDefinition taskDefinition, Map<String, List<SpecNode>> loadedTasks) {
        List<SpecNode> specNodes = loadedTasks.get(taskDefinition.getName());
        if (specNodes != null) {
            markSuccessProcess(specWorkflow, taskDefinition);
            log.info("loaded task {} from checkpoint", taskDefinition.getName());
            return specNodes;
        }
        return convertTaskDefinition(specWorkflow, taskDefinition);
    }

    private List<SpecNode> convertTaskDefinition(SpecWorkflow specWorkflow, TaskDefinition taskDefinition) {
        if (inSkippedList(taskDefinition)) {
            log.warn("task {} in skipped list", taskDefinition.getName());
            return Collections.emptyList();
        }

        try {
            AbstractParameterConverter converter = TaskConverterFactoryV2.create(converterProperties, specWorkflow, dagData, taskDefinition);
            SpecNode specNode = converter.convert();
            if (specNode != null) {
                DolphinSchedulerV2Context.getContext().getTaskCodeSpecNodeMap().put(taskDefinition.getCode(), specNode);
                return Arrays.asList(specNode);
            } else {
                return Collections.emptyList();
            }
        } catch (UnSupportedTypeException e) {
            markFailedProcess(taskDefinition, e.getMessage());
            if (Config.INSTANCE.isSkipUnSupportType()) {
                log.warn("task {} with type {} unsupported, skip", taskDefinition.getTaskType(), taskDefinition.getName());
                return Collections.emptyList();
            } else {
                throw e;
            }
        } catch (Throwable e) {
            log.error("task converter error, taskName {} ", taskDefinition.getName(), e);
            if (Config.INSTANCE.isTransformContinueWithError()) {
                return Collections.emptyList();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    protected void markSuccessProcess(SpecWorkflow workflow, TaskDefinition taskDefinition) {
        for (SpecNode node : workflow.getNodes()) {
            DolphinMetrics metrics = DolphinMetrics.builder()
                    .projectName(taskDefinition.getProjectName())
                    .projectCode(taskDefinition.getProjectCode())
                    .processName(processDefinition.getName())
                    .processCode(processDefinition.getCode())
                    .taskName(taskDefinition.getName())
                    .taskCode(taskDefinition.getCode())
                    .taskType(taskDefinition.getTaskType())
                    .build();
            metrics.setWorkflowName(workflow.getName());
            metrics.setDwName(node.getName());
            String type = node.getScript().getRuntime().getCommand();
            metrics.setDwType(type);
            TransformerContext.getCollector().markSuccessMiddleProcess(metrics);
        }
    }

    private boolean inSkippedList(TaskDefinition taskDefinition) {
        if (Config.INSTANCE.getSkipTypes().contains(taskDefinition.getTaskType())
                || Config.INSTANCE.getSkipTaskCodes().contains(String.valueOf(taskDefinition.getCode()))) {
            log.warn("task name {} code {} in skipped list", taskDefinition.getName(), taskDefinition.getCode());
            markSkippedProcess(taskDefinition);
            return true;
        } else {
            return false;
        }
    }

    protected void markFailedProcess(TaskDefinition taskDefinition, String errorMsg) {
        DolphinMetrics metrics = DolphinMetrics.builder()
                .projectName(taskDefinition.getProjectName())
                .projectCode(taskDefinition.getProjectCode())
                .processName(processDefinition.getName())
                .processCode(processDefinition.getCode())
                .taskName(taskDefinition.getName())
                .taskCode(taskDefinition.getCode())
                .taskType(taskDefinition.getTaskType())
                .build();
        metrics.setErrorMsg(errorMsg);
        TransformerContext.getCollector().markFailedMiddleProcess(metrics);
    }

    protected void markSkippedProcess(TaskDefinition taskDefinition) {
        DolphinMetrics metrics = DolphinMetrics.builder()
                .projectName(taskDefinition.getProjectName())
                .projectCode(taskDefinition.getProjectCode())
                .processName(processDefinition.getName())
                .processCode(processDefinition.getCode())
                .taskName(taskDefinition.getName())
                .taskCode(taskDefinition.getCode())
                .taskType(taskDefinition.getTaskType())
                .build();
        TransformerContext.getCollector().markSkippedProcess(metrics);
    }
}
