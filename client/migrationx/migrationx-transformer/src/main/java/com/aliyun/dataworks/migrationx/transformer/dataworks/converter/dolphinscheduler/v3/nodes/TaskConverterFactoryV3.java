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

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes;

import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.TaskType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.ProcessDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.DolphinSchedulerConverterContext;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.AbstractParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.ConditionsParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.CustomParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.DLCParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.DataxParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.DependentParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.HiveCliParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.HttpParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.MrParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.ProcedureParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.PythonParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.ShellParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.SparkParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.SqlParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.SqoopParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.SubProcessParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.SwitchParameterConverter;
import com.aliyun.migrationx.common.context.TransformerContext;
import com.aliyun.migrationx.common.exception.UnSupportedTypeException;
import com.aliyun.migrationx.common.metrics.DolphinMetrics;
import com.aliyun.migrationx.common.utils.Config;

public class TaskConverterFactoryV3 {
    public static AbstractParameterConverter create(
            DagData processMeta, TaskDefinition taskDefinition, DolphinSchedulerConverterContext converterContext) throws Throwable {

        TransformerContext.getCollector().incrementType(taskDefinition.getTaskType());

        if (Config.get().getTempTaskTypes().contains(taskDefinition.getTaskType())) {
            return new CustomParameterConverter(processMeta, taskDefinition, converterContext);
        }

        TaskType taskType;
        try {
            taskType = TaskType.of(taskDefinition.getTaskType());
        } catch (Exception e) {
            markUnSupportedTask(processMeta.getProcessDefinition(), taskDefinition);
            throw new UnSupportedTypeException(taskDefinition.getTaskType());
        }

        switch (taskType) {
            case PROCEDURE:
                return new ProcedureParameterConverter(processMeta, taskDefinition, converterContext);
            case SQL:
                return new SqlParameterConverter(processMeta, taskDefinition, converterContext);
            case PYTHON:
                return new PythonParameterConverter(processMeta, taskDefinition, converterContext);
            case CONDITIONS:
                return new ConditionsParameterConverter(processMeta, taskDefinition, converterContext);
            case SUB_PROCESS:
                return new SubProcessParameterConverter(processMeta, taskDefinition, converterContext);
            case DEPENDENT:
                return new DependentParameterConverter(processMeta, taskDefinition, converterContext);
            case SHELL:
                return new ShellParameterConverter(processMeta, taskDefinition, converterContext);
            case SWITCH:
                return new SwitchParameterConverter(processMeta, taskDefinition, converterContext);
            case HTTP:
                return new HttpParameterConverter(processMeta, taskDefinition, converterContext);
            case SPARK:
                return new SparkParameterConverter(processMeta, taskDefinition, converterContext);
            case MR:
                return new MrParameterConverter(processMeta, taskDefinition, converterContext);
            case HIVECLI:
                return new HiveCliParameterConverter(processMeta, taskDefinition, converterContext);
            case DATAX:
                return new DataxParameterConverter(processMeta, taskDefinition, converterContext);
            case SQOOP:
                return new SqoopParameterConverter(processMeta, taskDefinition, converterContext);
            case DLC:
                return new DLCParameterConverter(processMeta, taskDefinition, converterContext);
            default:
                markUnSupportedTask(processMeta.getProcessDefinition(), taskDefinition);
                throw new UnSupportedTypeException(taskDefinition.getTaskType());
        }
    }

    private static void markUnSupportedTask(ProcessDefinition processDefinition, TaskDefinition taskDefinition) {
        DolphinMetrics metrics = DolphinMetrics.builder()
                .projectName(processDefinition.getProjectName())
                .projectCode(processDefinition.getProjectCode())
                .processName(processDefinition.getName())
                .processCode(processDefinition.getCode())
                .taskName(taskDefinition.getName())
                .taskCode(taskDefinition.getCode())
                .taskType(taskDefinition.getTaskType())
                .build();
        TransformerContext.getCollector().markUnSupportedSpecProcess(metrics);
    }
}
