/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.logic.subprocess;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.SpecRefEntity;
import com.aliyun.dataworks.common.spec.domain.enums.SpecEntityType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.common.context.DolphinSchedulerV3ConverterContext;
import com.aliyun.dataworks.migrationx.transformer.flowspec.model.SpecRefEntityWrapper;
import org.apache.commons.collections4.ListUtils;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-06-26
 */
public class SubProcessParameterConverter extends BaseSubProcessParameterConverter {

    // the workflow entity that the current sub-workflow refers to
    private SpecRefEntityWrapper specRefEntity;

    public SubProcessParameterConverter(DataWorksWorkflowSpec spec, SpecWorkflow specWorkflow, TaskDefinition taskDefinition,
                                        DolphinSchedulerV3ConverterContext context) {
        super(spec, specWorkflow, taskDefinition, context);
    }

    @Override
    public SpecNode convert() {
        SpecNode specNode = newSpecNode(taskDefinition);
        String uuid = context.getUuidFromCode(parameter.getProcessDefinitionCode());
        SpecRefEntityWrapper specRefEntityWrapper = context.getSpecRefEntityMap().get(uuid);
        SpecEntityType specEntityType = Optional.ofNullable(specRefEntityWrapper).map(SpecRefEntityWrapper::getType).orElse(null);
        if (SpecEntityType.WORKFLOW.equals(specEntityType)) {
            this.specRefEntity = specRefEntityWrapper;
            convertParameter(specNode);
        }

        // hint: the node returned may not be the final result of the conversion
        return specNode;
    }

    /**
     * Each node translates the specific logic of the parameters
     */
    @Override
    protected void convertParameter(SpecNode specNode) {
        SpecWorkflow workflow = specRefEntity.getWorkflow();
        copySubWorkflowAndAddInnerNode(workflow, specNode);
    }

    private void copySubWorkflowAndAddInnerNode(SpecWorkflow refWorkflow, SpecNode subprocessNode) {
        // clone the workflow's inner nodes
        SpecWorkflow copyWorkflow = copyWorkflow(refWorkflow);
        List<SpecNode> specNodes = ListUtils.emptyIfNull(copyWorkflow.getInnerNodes());
        // reset name
        specNodes.forEach(node -> {
            Optional.ofNullable(node.getScript())
                .ifPresent(script -> script.setPath(getScriptPath(subprocessNode) + "_" + node.getName()));
        });

        getWorkflowNodeList().addAll(specNodes);
        getWorkflowDependencyList().addAll(ListUtils.emptyIfNull(copyWorkflow.getInnerDependencies()));

        // get head and tail node of the sub-workflow, help build dependencies of sub process
        buildHeadAndTailNode(specNodes);
    }

    private void buildHeadAndTailNode(List<SpecNode> specNodeList) {
        Set<String> allOutputId = ListUtils.emptyIfNull(specNodeList).stream()
            .map(SpecNode::getOutputs)
            .flatMap(Collection::stream)
            .filter(output -> output instanceof SpecRefEntity)
            .map(output -> (SpecRefEntity)output)
            .map(SpecRefEntity::getId)
            .collect(Collectors.toSet());
        Set<String> allIdSet = ListUtils.emptyIfNull(specNodeList).stream()
            .map(SpecRefEntity::getId)
            .collect(Collectors.toSet());
        Set<String> nonTailIdSet = new HashSet<>();

        specNodeList.forEach(node -> {
            boolean isHead = ListUtils.emptyIfNull(node.getInputs()).stream()
                .map(input -> ((SpecRefEntity)input).getId())
                .noneMatch(allOutputId::contains);
            if (isHead) {
                headList.add(newWrapper(node));
            } else {
                ListUtils.emptyIfNull(node.getInputs()).stream()
                    .filter(input -> input instanceof SpecNodeOutput)
                    .map(input -> ((SpecNodeOutput)input).getData())
                    .forEach(nonTailIdSet::add);
            }
        });
        allIdSet.removeAll(nonTailIdSet);
        specNodeList.forEach(node -> {
            if (allIdSet.contains(node.getId())) {
                tailList.add(newWrapper(node));
            }
        });
    }
}
