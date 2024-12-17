/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.common;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.TaskType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.parameters.AbstractParameters;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.common.context.DolphinSchedulerV3ConverterContext;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.logic.condition.ConditionsParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.logic.dependent.DependentParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.logic.subprocess.SubProcessParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.mr.MapReduceParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.python.PythonParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.shell.ShellParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.spark.SparkParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.sql.SqlParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.sqoop.SqoopParameterConverter;
import com.aliyun.migrationx.common.exception.BizException;
import com.aliyun.migrationx.common.exception.ErrorCode;
import lombok.extern.slf4j.Slf4j;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-06-05
 */
@Slf4j
@SuppressWarnings("unchecked")
public class ParameterConverterFactory {

    private ParameterConverterFactory() {

    }

    /**
     * create task converter
     *
     * @param context        config context
     * @param specWorkflow   specWorkflow
     * @param taskDefinition taskDefinition
     * @return a converter to convert task
     */
    public static <P extends AbstractParameters> AbstractParameterConverter<P> create(DataWorksWorkflowSpec spec,
                                                                                      SpecWorkflow specWorkflow,
                                                                                      TaskDefinition taskDefinition,
                                                                                      DolphinSchedulerV3ConverterContext context) {
        TaskType taskType = TaskType.of(taskDefinition.getTaskType());
        // try to find converter from context, user may override default converter for some type by config
        AbstractParameterConverter<AbstractParameters> converter = createFromContext(spec, specWorkflow, taskDefinition, taskType, context);
        if (converter != null) {
            return (AbstractParameterConverter<P>)converter;
        }
        // create default converter by task type
        return (AbstractParameterConverter<P>)Optional.of(taskType).map(type -> {
            switch (taskType) {
                case SQL:
                    return new SqlParameterConverter(spec, specWorkflow, taskDefinition, context);
                case PYTHON:
                    return new PythonParameterConverter(spec, specWorkflow, taskDefinition, context);
                case SPARK:
                    return new SparkParameterConverter(spec, specWorkflow, taskDefinition, context);
                case CONDITIONS:
                    return new ConditionsParameterConverter(spec, specWorkflow, taskDefinition, context);
                case SUB_PROCESS:
                    return new SubProcessParameterConverter(spec, specWorkflow, taskDefinition, context);
                case DEPENDENT:
                    return new DependentParameterConverter(spec, specWorkflow, taskDefinition, context);
                case SHELL:
                    return new ShellParameterConverter(spec, specWorkflow, taskDefinition, context);
                case MR:
                    return new MapReduceParameterConverter(spec, specWorkflow, taskDefinition, context);
                case SQOOP:
                    return new SqoopParameterConverter(spec, specWorkflow, taskDefinition, context);
                default:
                    throw new BizException(ErrorCode.UNKNOWN_ENUM_TYPE, TaskType.class, taskType);
            }
        }).orElseThrow(() -> new BizException(ErrorCode.PARAMETER_NOT_SET, "taskDefinition.taskType"));
    }

    private static <P extends AbstractParameters> AbstractParameterConverter<P> createFromContext(DataWorksWorkflowSpec spec,
                                                                                                  SpecWorkflow specWorkflow,
                                                                                                  TaskDefinition taskDefinition,
                                                                                                  TaskType taskType,
                                                                                                  DolphinSchedulerV3ConverterContext context) {
        if (taskType == null) {
            log.warn("task type is null, taskDefinition: {}", taskDefinition);
            return null;
        }

        // find converter class name from node type map in context, user may override default converter for some task type
        Map<String, String> nodeTypeMap = Optional.ofNullable(context).map(DolphinSchedulerV3ConverterContext::getNodeTypeMap).orElse(
            Collections.emptyMap());
        String converterClassName = nodeTypeMap.get(taskType.name());
        if (converterClassName == null) {
            return null;
        }

        // use reflection to create instance
        try {
            Class<?> clazz = Class.forName(converterClassName);
            Constructor<?> declaredConstructor = clazz.getDeclaredConstructor(DataWorksWorkflowSpec.class, SpecWorkflow.class, TaskDefinition.class,
                DolphinSchedulerV3ConverterContext.class);
            return (AbstractParameterConverter<P>)declaredConstructor.newInstance(spec, specWorkflow, taskDefinition, context);
        } catch (ClassNotFoundException e) {
            log.warn("class not found: {}", converterClassName, e);
        } catch (NoSuchMethodException e) {
            log.warn("construct method not found: {}", converterClassName, e);
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
            log.warn("error found when new instance, {}", converterClassName, e);
        }
        return null;
    }
}
