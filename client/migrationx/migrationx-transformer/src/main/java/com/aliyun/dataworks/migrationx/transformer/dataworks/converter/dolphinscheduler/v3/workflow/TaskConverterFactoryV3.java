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

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow;

import java.util.Properties;

import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.TaskType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.AbstractParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.CustomParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.DataxParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.DependentParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.HiveCliParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.MrParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.ProcedureParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.PythonParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.ShellParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.SparkParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.SqlParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.SqoopParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.SubProcessParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.SwitchParameterConverter;
import com.aliyun.migrationx.common.exception.UnSupportedTypeException;
import com.aliyun.migrationx.common.utils.Config;

public class TaskConverterFactoryV3 {
    public static AbstractParameterConverter create(
            Properties properties, SpecWorkflow specWorkflow,
            DagData processMeta, TaskDefinition taskDefinition) throws Throwable {
        if (Config.INSTANCE.getTempTaskTypes().contains(taskDefinition.getTaskType())) {
            return new CustomParameterConverter(properties, specWorkflow, processMeta, taskDefinition);
        }
        TaskType taskType = TaskType.of(taskDefinition.getTaskType());
        if (taskType == null) {
            throw new UnSupportedTypeException(taskDefinition.getTaskType());
        }

        switch (taskType) {
            case SHELL:
                return new ShellParameterConverter(properties, specWorkflow, processMeta, taskDefinition);
            case PYTHON:
                return new PythonParameterConverter(properties, specWorkflow, processMeta, taskDefinition);
            case SQL:
                return new SqlParameterConverter(properties, specWorkflow, processMeta, taskDefinition);
            case MR:
                return new MrParameterConverter(properties, specWorkflow, processMeta, taskDefinition);
            case SPARK:
                return new SparkParameterConverter(properties, specWorkflow, processMeta, taskDefinition);
            case SUB_PROCESS:
                return new SubProcessParameterConverter(properties, specWorkflow, processMeta, taskDefinition);
            case HIVECLI:
                return new HiveCliParameterConverter(properties, specWorkflow, processMeta, taskDefinition);
            case DEPENDENT:
                return new DependentParameterConverter(properties, specWorkflow, processMeta, taskDefinition);
            case SWITCH:
                return new SwitchParameterConverter(properties, specWorkflow, processMeta, taskDefinition);
            case SQOOP:
                return new SqoopParameterConverter(properties, specWorkflow, processMeta, taskDefinition);
            case DATAX:
                return new DataxParameterConverter(properties, specWorkflow, processMeta, taskDefinition);
            case PROCEDURE:
                return new ProcedureParameterConverter(properties, specWorkflow, processMeta, taskDefinition);
            default:
                throw new UnSupportedTypeException(taskDefinition.getTaskType());
        }
    }
}
