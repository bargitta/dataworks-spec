/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.dolphinscheduler.converter.flowspec.common;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.parameters.AbstractParameters;
import com.aliyun.dataworks.migrationx.transformer.dolphinscheduler.converter.flowspec.common.context.FlowSpecConverterContext;
import com.aliyun.dataworks.migrationx.transformer.dolphinscheduler.converter.flowspec.python.PythonNodeConverter;
import com.aliyun.dataworks.migrationx.transformer.dolphinscheduler.converter.flowspec.shell.ShellNodeConverter;
import com.aliyun.dataworks.migrationx.transformer.dolphinscheduler.converter.flowspec.spark.SparkNodeConverter;
import com.aliyun.dataworks.migrationx.transformer.dolphinscheduler.converter.flowspec.sql.SqlNodeConverter;
import com.aliyun.migrationx.common.exception.BizException;
import com.aliyun.migrationx.common.exception.ErrorCode;
import lombok.extern.slf4j.Slf4j;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-07-04
 */
@Slf4j
@SuppressWarnings("unchecked")
public class NodeConverterFactory {

    private NodeConverterFactory() {

    }

    /**
     * create node converter by spec node type
     *
     * @param specNode spec node
     * @param context  context
     * @return converter
     */
    public static AbstractNodeConverter<? extends AbstractParameters> create(SpecNode specNode, FlowSpecConverterContext context) {
        CodeProgramType codeProgramType = Optional.of(specNode)
            .map(SpecNode::getScript)
            .map(SpecScript::getRuntime)
            .map(SpecScriptRuntime::getCommand)
            .map(CodeProgramType::getNodeTypeByName)
            .orElseThrow(() -> new BizException(ErrorCode.PARAMETER_NOT_SET, "specNode.script.runtime.command"));
        AbstractNodeConverter<? extends AbstractParameters> converter = createFromContext(specNode, codeProgramType, context);
        if (converter != null) {
            return converter;
        }
        switch (codeProgramType) {
            case ODPS_SQL:
                return new SqlNodeConverter(specNode, context);
            case PYODPS:
            case PYODPS3:
                return new PythonNodeConverter(specNode, context);
            case SHELL:
            case DIDE_SHELL:
                return new ShellNodeConverter(specNode, context);
            case ODPS_SPARK:
                return new SparkNodeConverter(specNode, context);
            default:
                throw new BizException(ErrorCode.UNKNOWN_ENUM_TYPE, "CodeProgramType", codeProgramType);
        }
    }

    private static <P extends AbstractParameters> AbstractNodeConverter<P> createFromContext(SpecNode specNode,
                                                                                             CodeProgramType codeProgramType,
                                                                                             FlowSpecConverterContext context) {
        // find converter class name from node type map in context
        Map<String, String> nodeTypeMap = Optional.ofNullable(context).map(FlowSpecConverterContext::getNodeTypeMap).orElse(
            Collections.emptyMap());
        String converterClassName = nodeTypeMap.get(codeProgramType.getName());
        if (converterClassName == null) {
            return null;
        }
        // use reflection to create instance
        try {
            Class<?> clazz = Class.forName(converterClassName);
            Constructor<?> declaredConstructor = clazz.getDeclaredConstructor(SpecNode.class, FlowSpecConverterContext.class);
            return (AbstractNodeConverter<P>)declaredConstructor.newInstance(specNode, context);
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
