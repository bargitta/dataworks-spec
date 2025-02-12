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

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v1.workflow;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.dw.codemodel.ControllerBranchCode.Branch;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.noref.SpecBranches;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecTrigger;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.DolphinSchedulerV1Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.ProcessData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.ProcessMeta;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.TaskNode;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.task.AbstractParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.utils.CronExpressUtil;
import com.aliyun.dataworks.migrationx.transformer.core.checkpoint.CheckPoint;
import com.aliyun.dataworks.migrationx.transformer.core.checkpoint.StoreWriter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.filters.DolphinSchedulerConverterFilter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v1.workflow.parameters.AbstractParameterConverter;
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
public class V1ProcessDefinitionConverter {
    private List<SpecNode> specNodes = new ArrayList<>();

    private static final SpecScriptRuntime WORKFLOW_RUNTIME = new SpecScriptRuntime();

    private static final SpecScriptRuntime MANUAL_WORKFLOW_RUNTIME = new SpecScriptRuntime();

    static {
        WORKFLOW_RUNTIME.setEngine(CodeProgramType.VIRTUAL_WORKFLOW.getCalcEngineType().getLabel());
        WORKFLOW_RUNTIME.setCommand("WORKFLOW");

        MANUAL_WORKFLOW_RUNTIME.setEngine(CodeProgramType.VIRTUAL_WORKFLOW.getCalcEngineType().getLabel());
        MANUAL_WORKFLOW_RUNTIME.setCommand("MANUAL_WORKFLOW");
    }

    private ProcessMeta processDefinition;
    private Properties converterProperties;
    private List<TaskNode> taskDefinitionList;

    private CheckPoint checkPoint;
    private SpecWorkflow specWorkflow;

    private DolphinSchedulerConverterFilter filter;

    public V1ProcessDefinitionConverter(ProcessMeta processMeta, List<TaskNode> taskDefinitionList, Properties converterProperties) {
        this.processDefinition = processMeta;
        this.converterProperties = converterProperties;
        this.taskDefinitionList = taskDefinitionList;
        this.filter = new DolphinSchedulerConverterFilter();
        checkPoint = CheckPoint.getInstance();
    }

    public static String toWorkflowName(ProcessMeta processMeta) {
        return com.aliyun.dataworks.migrationx.domain.dataworks.utils.StringUtils.toValidName(Joiner.on("_").join(
                processMeta.getProjectName(), processMeta.getProcessDefinitionName()));
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
        specWorkflow = convertProcess(processDefinition);
        DolphinSchedulerV1Context.getContext().getSubProcessCodeWorkflowMap().put(processDefinition.getProcessDefinitionId(), specWorkflow);
        convertTasks();
        convertTrigger(specWorkflow);
        //convertTaskRelations(specWorkflow);
        handleBranch(specWorkflow);
        return specWorkflow;
    }

    protected SpecWorkflow convertProcess(ProcessMeta processDefinition) {
        log.info("convert workflow,processDefinition: {}", processDefinition.getProcessDefinitionName());

        SpecWorkflow specWorkflow = initWorkflow();
        specWorkflow.setId(UuidGenerators.generateUuid());
        specWorkflow.setName(toWorkflowName(processDefinition));
        specWorkflow.setDescription(processDefinition.getProcessDefinitionDescription());

        ProcessData processData = processDefinition.getProcessDefinitionJson();

        List<SpecVariable> specVariableList = new ParamListConverter(processData.getGlobalParams()).convert();
        log.info("convert workflow,global params: {}", specVariableList);

        SpecScript script = new SpecScript();
        script.setParameters(specVariableList);
        script.setRuntime(WORKFLOW_RUNTIME);

        //todo
        script.setPath(getScriptPath(specWorkflow.getName()));
        specWorkflow.setScript(script);

        specWorkflow.getOutputs().add(buildDefaultOutput(specWorkflow));
        return specWorkflow;
    }

    protected String getScriptPath(String name) {
        String defaultPath = StringUtils.defaultString(Config.get().getBasePath(), StringUtils.EMPTY);
        return FilenameUtils.concat(defaultPath, name);
    }

    protected List<SpecNode> convertTaskDefinitions(SpecWorkflow specWorkflow) {
        return convertTasks();
    }

    protected void convertTrigger(SpecWorkflow specWorkflow) {
        processDefinition.getScheduleCrontab();
        String crontab = processDefinition.getScheduleCrontab();
        if (StringUtils.isNotBlank(crontab)) {
            SpecTrigger trigger = new SpecTrigger();
            trigger.setCron(convertCrontab(crontab));
            specWorkflow.setTrigger(trigger);
            log.info("convert workflow,crontab: {}", crontab);
        }
    }

    protected String convertCrontab(String scheduleCrontab) {
        try {
            return CronExpressUtil.quartzCronExpressionToDwCronExpress(scheduleCrontab);
        } catch (ParseException e) {
            log.error("convert quartz cron expression error: ", e);
        }

        return "day";
    }

    /**
     * when handling task, spec node maybe not exists, post handle branch
     * change branch output data (task code) to specNode id
     */
    private void handleBranch(SpecWorkflow specWorkflow) {
        Map<String, Object> taskCodeSpecNodeMap = DolphinSchedulerV1Context.getContext().getTaskCodeSpecNodeMap();
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


    private SpecNodeOutput buildDefaultOutput(SpecWorkflow specWorkflow) {
        SpecNodeOutput specNodeOutput = new SpecNodeOutput();
        specNodeOutput.setIsDefault(true);
        specNodeOutput.setData(specWorkflow.getId());
        specNodeOutput.setRefTableName(specWorkflow.getName());
        return specNodeOutput;
    }

    public List<SpecNode> convertTasks() {
        String projectName = processDefinition.getProjectName();
        String processName = processDefinition.getProcessDefinitionName();
        Map<String, List<SpecNode>> loadedTasks = checkPoint.loadFromCheckPoint(projectName, processName);
        SpecWorkflow workflow = new SpecWorkflow();

        final Function<StoreWriter, List<SpecNode>> processFunc = (StoreWriter writer) ->
                Optional.ofNullable(processDefinition.getProcessDefinitionJson())
                        .map(pd -> toSpecNodes(pd, loadedTasks, writer))
                        .orElse(Collections.emptyList());

        List<SpecNode> specNodes = checkPoint.doWithCheckpoint(processFunc, projectName);

        //processSubProcessDefinitionDepends();
        return specNodes;
    }

    private List<SpecNode> toSpecNodes(ProcessData pd, Map<String, List<SpecNode>> loadedTasks, StoreWriter writer) {
        return ListUtils.emptyIfNull(pd.getTasks())
                .stream()
                .filter(s -> {
                    if (Thread.currentThread().isInterrupted()) {
                        throw new RuntimeException(new InterruptedException());
                    }
                    return true;
                })
                .filter(task -> {
                    boolean willConvert = filter.filterTasks(processDefinition.getProjectName(),
                            processDefinition.getProcessDefinitionName(), task.getName());
                    if (!willConvert) {
                        log.warn("task {} not in filterTasks list", task.getName());
                    }
                    return willConvert;
                })
                .filter(taskNode -> !inSkippedList(taskNode))
                .map(taskNode -> {
                    List<SpecNode> specNodes = convertTaskToSpecNodeWithLoadedTask(taskNode, loadedTasks);
                    checkPoint.doCheckpoint(writer, specNodes, processDefinition.getProcessDefinitionName(), taskNode.getName());
                    return specNodes;
                })
                .collect(Collectors.toList())
                .stream().flatMap(List::stream)
                .collect(Collectors.toList());
    }

    private List<SpecNode> convertTaskToSpecNodeWithLoadedTask(TaskNode taskNode, Map<String, List<SpecNode>> loadedTasks) {
        List<SpecNode> workflows = loadedTasks.get(taskNode.getName());
        if (workflows != null) {
            markSuccessProcess(workflows, taskNode);
            log.info("loaded task {} from checkpoint", taskNode.getName());
            return workflows;
        }
        return converter(taskNode);
    }

    private List<SpecNode> converter(TaskNode taskNode) {
        AbstractParameterConverter<AbstractParameters> converter;
        try {
            converter = TaskConverterFactoryV1.create(converterProperties, this.specWorkflow, processDefinition, taskNode);
            SpecNode specNode = converter.convert();
            if (specNode != null) {
                DolphinSchedulerV1Context.getContext().getTaskCodeSpecNodeMap().put(taskNode.getId(), specNode);
                return Arrays.asList(specNode);
            } else {
                return Collections.emptyList();
            }
        } catch (UnSupportedTypeException e) {
            markFailedProcess(taskNode, e.getMessage());
            if (Config.get().isSkipUnSupportType()) {
                List<SpecNode> list = Collections.emptyList();
                return list;
            } else {
                throw e;
            }
        } catch (Throwable e) {
            log.error("task converter error: ", e);
            if (Config.get().isTransformContinueWithError()) {
                return Collections.emptyList();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    protected void markSuccessProcess(List<SpecNode> specNodes, TaskNode taskNode) {
        for (SpecNode node : specNodes) {
            DolphinMetrics metrics = DolphinMetrics.builder()
                    .projectName(processDefinition.getProjectName())
                    .processName(processDefinition.getProcessDefinitionName())
                    .taskName(taskNode.getName())
                    .taskType(taskNode.getType().name())
                    .build();
            metrics.setWorkflowName(this.specWorkflow.getName());
            metrics.setDwName(node.getName());
            metrics.setDwType(taskNode.getType().name());
            TransformerContext.getCollector().markSuccessMiddleProcess(metrics);
        }
    }

    private boolean inSkippedList(TaskNode taskNode) {
        if (Config.get().getSkipTypes().contains(taskNode.getType())
                || Config.get().getSkipTaskCodes().contains(taskNode.getName())) {
            log.warn("task name {}  in skipped list", taskNode.getName());
            markSkippedProcess(taskNode);
            return true;
        } else {
            return false;
        }
    }

    protected void markSkippedProcess(TaskNode taskNode) {
        DolphinMetrics metrics = DolphinMetrics.builder()
                .projectName(processDefinition.getProjectName())
                .processName(processDefinition.getProcessDefinitionName())
                .taskName(taskNode.getName())
                .taskType(taskNode.getType().name())
                .timestamp(System.currentTimeMillis())
                .build();
        TransformerContext.getCollector().markSkippedProcess(metrics);
    }

    protected void markFailedProcess(TaskNode taskNode, String errorMsg) {
        DolphinMetrics metrics = DolphinMetrics.builder()
                .projectName(processDefinition.getProjectName())
                .processName(processDefinition.getProcessDefinitionName())
                .taskName(taskNode.getName())
                .taskType(taskNode.getType().name())
                .timestamp(System.currentTimeMillis())
                .build();
        metrics.setErrorMsg(errorMsg);
        TransformerContext.getCollector().markSkippedProcess(metrics);
    }
}
