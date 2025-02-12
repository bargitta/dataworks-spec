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

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v2.nodes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.Project;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.DolphinSchedulerV2Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.ProcessDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.Schedule;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.entity.DataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.entity.ResourceComponent;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.entity.UdfFunc;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.ReleaseState;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.task.AbstractParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DwWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.Node;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.WorkflowParameter;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.types.WorkflowType;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.types.WorkflowVersion;
import com.aliyun.dataworks.migrationx.transformer.core.checkpoint.CheckPoint;
import com.aliyun.dataworks.migrationx.transformer.core.checkpoint.StoreWriter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.DolphinSchedulerConverterContext;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.filters.DolphinSchedulerConverterFilter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.utils.DolphinFilter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v1.nodes.parameters.ProcessDefinitionConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v2.nodes.parameters.AbstractParameterConverter;
import com.aliyun.migrationx.common.context.TransformerContext;
import com.aliyun.migrationx.common.exception.UnSupportedTypeException;
import com.aliyun.migrationx.common.metrics.DolphinMetrics;
import com.aliyun.migrationx.common.utils.Config;
import com.aliyun.migrationx.common.utils.GsonUtils;

import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;

/**
 * @author 聿剑
 * @date 2022/10/19
 */
@Slf4j
public class V2ProcessDefinitionConverter
        extends ProcessDefinitionConverter<Project, DagData, DataSource, ResourceComponent, UdfFunc> {
    private List<DwWorkflow> dwWorkflowList = new ArrayList<>();

    private ProcessDefinition processDefinition;
    private DagData dagData;

    private List<TaskDefinition> taskDefinitionList;

    private DolphinSchedulerConverterFilter filter;

    public V2ProcessDefinitionConverter(
            DolphinSchedulerConverterContext<Project, DagData, DataSource, ResourceComponent, UdfFunc> converterContext,
            DagData dagData) {
        super(converterContext, dagData);
        this.dagData = dagData;
        this.processDefinition = dagData.getProcessDefinition();
        this.taskDefinitionList = dagData.getTaskDefinitionList();
        this.filter = new DolphinSchedulerConverterFilter();
    }

    public static String toWorkflowName(ProcessDefinition processDefinition) {
        return com.aliyun.dataworks.migrationx.domain.dataworks.utils.StringUtils.toValidName(Joiner.on("_").join(
                processDefinition.getProjectName(), processDefinition.getName()));
    }

    private boolean willConvert() {
        List<String> codes = DolphinSchedulerV2Context.getContext().getSubProcessCodeMap(processDefinition.getCode());
        ReleaseState releaseState = processDefinition.getReleaseState();
        return DolphinFilter.willConvert(processDefinition.getName(), releaseState == null ? null : releaseState.name(), codes);
    }

    @Override
    public List<DwWorkflow> convert() {
        log.info("converting processDefinition {}", processDefinition.getName());
        if (!willConvert()) {
            return Collections.emptyList();
        }

        DwWorkflow dwWorkflow = new DwWorkflow();
        dwWorkflow.setName(toWorkflowName(processDefinition));
        dwWorkflow.setType(WorkflowType.BUSINESS);
        dwWorkflow.setScheduled(true);
        dwWorkflow.setVersion(WorkflowVersion.V3);

        converterContext.setDwWorkflow(dwWorkflow);
        dwWorkflow.setParameters(getGlobalParams());

        CheckPoint checkPoint = CheckPoint.getInstance();
        String projectName = processDefinition.getProjectName();
        String processName = processDefinition.getName();

        Map<String, List<DwWorkflow>> loadedTasks = checkPoint.loadFromCheckPoint(projectName, processName);

        final Function<StoreWriter, List<DwWorkflow>> processFunc = (StoreWriter writer) ->
                ListUtils.emptyIfNull(taskDefinitionList)
                        .stream()
                        .filter(s -> {
                            if (Thread.currentThread().isInterrupted()) {
                                throw new RuntimeException(new InterruptedException());
                            }
                            return true;
                        })
                        .filter(task -> {
                            //check if task in filter while list, if filter is not empty, task will convert only when task in filter
                            boolean willConvert = filter.filterTasks(projectName, processName, task.getName());
                            if (!willConvert) {
                                log.warn("task {} not in filter pattern, skip", task.getName());
                            }
                            return willConvert;
                        })
                        .map(task -> {
                            List<DwWorkflow> dwWorkflows = convertTaskToWorkflowWithLoadedTask(task, loadedTasks);
                            checkPoint.doCheckpoint(writer, dwWorkflows, processName, task.getName());
                            return dwWorkflows;
                        })
                        .flatMap(List::stream)
                        .collect(Collectors.toList());

        dwWorkflowList = checkPoint.doWithCheckpoint(processFunc, projectName);

        if (dagData.getSchedule() != null) {
            setSchedule(dwWorkflow);
        }

        return dwWorkflowList;
    }

    private List<DwWorkflow> convertTaskToWorkflowWithLoadedTask(TaskDefinition taskDefinition, Map<String, List<DwWorkflow>> loadedTasks) {
        List<DwWorkflow> workflows = loadedTasks.get(taskDefinition.getName());
        if (workflows != null) {
            markSuccessProcess(workflows, taskDefinition);
            log.info("loaded task {} from checkpoint", taskDefinition.getName());
            return workflows;
        }
        return convertTaskToWorkFlow(taskDefinition);
    }

    protected void markSuccessProcess(List<DwWorkflow> workflows, TaskDefinition taskDefinition) {
        for (DwWorkflow workflow : workflows) {
            for (Node node : workflow.getNodes()) {
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
                metrics.setDwType(node.getType());
                TransformerContext.getCollector().markSuccessMiddleProcess(metrics);
            }
        }
    }

    private void setSchedule(DwWorkflow dwWorkflow) {
        Schedule schedule = dagData.getSchedule();
        ListUtils.emptyIfNull(dwWorkflow.getNodes()).forEach(node ->
        {
            if (schedule.getCrontab() != null) {
                node.setCronExpress(convertCrontab(schedule.getCrontab()));
            }
            if (schedule.getStartTime() != null) {
                node.setStartEffectDate(schedule.getStartTime());
            }
            if (schedule.getEndTime() != null) {
                node.setEndEffectDate(schedule.getEndTime());
            }
        });
    }

    private String getGlobalParams() {
        List<WorkflowParameter> parameters = MapUtils.emptyIfNull(processDefinition.getGlobalParamMap()).entrySet().stream().map(entry ->
                        new WorkflowParameter().setKey(entry.getKey()).setValue(entry.getValue()))
                .collect(Collectors.toList());
        return GsonUtils.toJsonString(parameters);
    }

    private List<DwWorkflow> convertTaskToWorkFlow(TaskDefinition taskDefinition) {
        if (inSkippedList(taskDefinition)) {
            return Collections.emptyList();
        }
        try {
            AbstractParameterConverter<AbstractParameters> taskConverter
                    = TaskConverterFactoryV2.create(dagData, taskDefinition, converterContext);
            taskConverter.convert();
            return taskConverter.getWorkflowList();
        } catch (UnSupportedTypeException e) {
            markFailedProcess(taskDefinition, e.getMessage());
            if (Config.get().isSkipUnSupportType()) {
                log.error("task name {} with type {} unsupported, skip", taskDefinition.getName(), taskDefinition.getTaskType());
                return Collections.emptyList();
            } else {
                throw e;
            }
        } catch (Throwable e) {
            log.error("task converter error, taskName {} ", taskDefinition.getName(), e);
            if (Config.get().isTransformContinueWithError()) {
                return Collections.emptyList();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private boolean inSkippedList(TaskDefinition taskDefinition) {
        if (Config.get().getSkipTypes().contains(taskDefinition.getTaskType())
                || Config.get().getSkipTaskCodes().contains(String.valueOf(taskDefinition.getCode()))) {
            log.warn("task name {} code {} in skipped list", taskDefinition.getName(), taskDefinition.getCode());
            markSkippedProcess(taskDefinition);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public List<DwWorkflow> getWorkflowList() {
        return dwWorkflowList;
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
}
