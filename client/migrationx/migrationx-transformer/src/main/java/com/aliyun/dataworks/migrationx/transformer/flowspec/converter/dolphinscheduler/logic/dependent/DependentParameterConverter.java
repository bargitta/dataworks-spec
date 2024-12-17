/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.logic.dependent;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.dw.codemodel.ControllerJoinCode;
import com.aliyun.dataworks.common.spec.domain.dw.codemodel.ControllerJoinCode.Status;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.SpecEntityType;
import com.aliyun.dataworks.common.spec.domain.noref.SpecJoin;
import com.aliyun.dataworks.common.spec.domain.noref.SpecJoinBranch;
import com.aliyun.dataworks.common.spec.domain.noref.SpecLogic;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.DependentRelation;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.DependentItem;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.DependentTaskModel;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.v320.task.Dependence;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.v320.task.Dependence.DependentFailurePolicyEnum;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.v320.task.dependent.DependentParameters;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.common.AbstractParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.common.context.DolphinSchedulerV3ConverterContext;
import com.aliyun.dataworks.migrationx.transformer.flowspec.model.SpecRefEntityWrapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.BooleanUtils;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-06-26
 */
public class DependentParameterConverter extends AbstractParameterConverter<DependentParameters> {

    private static final SpecScriptRuntime RUNTIME = new SpecScriptRuntime();

    static {
        RUNTIME.setEngine(CodeProgramType.CONTROLLER_JOIN.getCalcEngineType().getLabel());
        RUNTIME.setCommand(CodeProgramType.CONTROLLER_JOIN.getName());
    }

    public DependentParameterConverter(DataWorksWorkflowSpec spec, SpecWorkflow specWorkflow, TaskDefinition taskDefinition,
                                       DolphinSchedulerV3ConverterContext context) {
        super(spec, specWorkflow, taskDefinition, context);
    }

    /**
     * Each node translates the specific logic of the parameters
     */
    @Override
    protected void convertParameter(SpecNode finalJoinNode) {
        // need wait to check, so we use rerun times to wait to check
        DependentFailurePolicyEnum dependentFailurePolicyEnum = Optional.ofNullable(parameter).map(DependentParameters::getDependence)
            .map(Dependence::getFailurePolicy).orElse(null);
        if (DependentFailurePolicyEnum.DEPENDENT_FAILURE_WAITING.equals(dependentFailurePolicyEnum)) {
            Integer rerunInterval = finalJoinNode.getRerunInterval();
            if (rerunInterval == null || rerunInterval <= 0) {
                finalJoinNode.setRerunInterval((int)Duration.ofMinutes(1).toMillis());
            }
            finalJoinNode.setRerunTimes(
                (int)(parameter.getDependence().getFailureWaitingTime() / Duration.ofMillis(finalJoinNode.getRerunInterval()).toMinutes()));
            resetNodeStrategy(finalJoinNode);
        }
        /*
          if getJudgeConditionOnce is true, the node will not be rerun to wait condition to be success, otherwise, it will follow the dolphin
          scheduler config
         */
        if (BooleanUtils.isTrue(context.getJudgeConditionOnce())) {
            finalJoinNode.setRerunTimes(0);
            finalJoinNode.setRerunInterval(0);
            resetNodeStrategy(finalJoinNode);
        }

        SpecScript script = new SpecScript();
        script.setId(generateUuid());
        script.setRuntime(RUNTIME);
        script.setPath(getScriptPath(finalJoinNode));
        finalJoinNode.setScript(script);

        // build main content
        Map<String, SpecWorkflow> workflowIdMap = buildWorkFlowIdMap();
        Optional.ofNullable(parameter).map(DependentParameters::getDependence).ifPresent(dependence -> {
            List<SpecNode> nodeList = new ArrayList<>();
            for (int i = 0; i < ListUtils.emptyIfNull(dependence.getDependTaskList()).size(); i++) {
                DependentTaskModel dependentTaskModel = dependence.getDependTaskList().get(i);
                List<SpecNode> subNodeList = ListUtils.emptyIfNull(dependentTaskModel.getDependItemList()).stream()
                    .map(this::findSubNode)
                    .filter(Objects::nonNull)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
                // if spec node depend on the whole workflow, need depend on workflow output
                List<SpecNodeOutput> subNodeOutputList = ListUtils.emptyIfNull(dependentTaskModel.getDependItemList()).stream()
                    .map(dependItem -> findSubNodeOutput(workflowIdMap, dependItem))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());

                SpecJoin specJoin = newSpecJoin(subNodeList, subNodeOutputList, dependentTaskModel.getRelation());
                // we can reduce a layer when the size of dependTaskList is 1
                SpecNode specNode = dependence.getDependTaskList().size() <= 1 ?
                    finalJoinNode : copyJoinNode(finalJoinNode, specJoin, "_join_" + i);

                if (specNode.getJoin() == null) {
                    specNode.setJoin(specJoin);
                    Optional.ofNullable(specNode.getScript()).ifPresent(s -> s.setContent(buildControllerJoinCode(specNode).getContent()));
                }

                addRelation(specNode, subNodeList, subNodeOutputList);
                nodeList.add(specNode);
            }
            // join node not in node list, join node is downstream of nodes in nodeList
            if (CollectionUtils.size(nodeList) > 1) {
                SpecJoin specJoin = newSpecJoin(nodeList, null, dependence.getRelation());
                finalJoinNode.setJoin(specJoin);
                Optional.ofNullable(buildControllerJoinCode(finalJoinNode)).map(ControllerJoinCode::getContent).ifPresent(script::setContent);
                addRelation(finalJoinNode, nodeList);
            }
        });
    }

    /**
     * This method mainly deals with cases that depend on the whole workflow
     *
     * @param dependentItem dependentItem
     * @return SpecNode, may be a join node
     */
    private List<SpecNode> findSubNode(DependentItem dependentItem) {
        if (dependentItem.getDepTaskCode() == 0L) {
            // depend on the whole workflow, don't deal this time
            return Collections.emptyList();
        }

        List<SpecRefEntityWrapper> specRefEntityWrappers = context.getEntityTailMap().get(dependentItem.getDepTaskCode());
        return ListUtils.emptyIfNull(specRefEntityWrappers).stream()
            .map(SpecRefEntityWrapper::getNode)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    private List<SpecNodeOutput> findSubNodeOutput(Map<String, SpecWorkflow> workflowIdMap, DependentItem dependentItem) {
        List<SpecNodeOutput> res = new ArrayList<>();

        if (dependentItem.getDepTaskCode() == 0L) {
            // depend on the whole workflow, find the workflow output
            String id = context.getUuidFromCode(dependentItem.getDefinitionCode());
            SpecWorkflow specWorkflow = workflowIdMap.get(id);
            Optional.ofNullable(specWorkflow).map(flow -> getDefaultOutput(specWorkflow, false)).ifPresent(res::add);
        } else {
            String id = context.getUuidFromCode(dependentItem.getDepTaskCode());
            SpecRefEntityWrapper specRefEntityWrapper = context.getSpecRefEntityMap().get(id);
            if (SpecEntityType.WORKFLOW.equals(specRefEntityWrapper.getType())) {
                SpecWorkflow specWorkflow = specRefEntityWrapper.getWorkflow();
                Optional.ofNullable(specWorkflow).map(flow -> getDefaultOutput(specWorkflow, false)).ifPresent(res::add);
            }
        }
        return res;
    }

    private SpecJoin newSpecJoin(List<SpecNode> specNodeList, List<SpecNodeOutput> specNodeOutputList, DependentRelation relation) {
        SpecJoin specJoin = new SpecJoin();
        specJoin.setBranches(new ArrayList<>());
        List<String> branchNameList = new ArrayList<>();
        ListUtils.emptyIfNull(specNodeList).forEach(specNode -> {
            SpecJoinBranch specJoinBranch = buildSpecJoinBranch(specNode, Status.SUCCESS);
            specJoin.getBranches().add(specJoinBranch);
            branchNameList.add(specJoinBranch.getName());
        });
        ListUtils.emptyIfNull(specNodeOutputList).forEach(specNodeOutput -> {
            SpecJoinBranch specJoinBranch = buildSpecJoinBranch(specNodeOutput, Status.SUCCESS);
            specJoin.getBranches().add(specJoinBranch);
            branchNameList.add(specJoinBranch.getName());
        });
        SpecLogic specLogic = new SpecLogic();
        specLogic.setExpression(String.join(" " + relation.name() + " ", branchNameList));
        specJoin.setLogic(specLogic);
        specJoin.setResultStatus(Status.SUCCESS.getCode());
        return specJoin;
    }

    private Map<String, SpecWorkflow> buildWorkFlowIdMap() {
        List<SpecWorkflow> workflows = new ArrayList<>(Collections.singleton(getWorkFlow()));
        // workflows in config file
        ListUtils.emptyIfNull(context.getDependSpecification()).stream()
            .map(Specification::getSpec)
            .filter(Objects::nonNull)
            .map(DataWorksWorkflowSpec::getWorkflows)
            .filter(Objects::nonNull)
            .flatMap(List::stream)
            .forEach(workflows::add);

        // workflows which were converted before and store in context
        context.getSpecRefEntityMap().values().stream()
            .filter(specRefEntityWrapper -> SpecEntityType.WORKFLOW.equals(specRefEntityWrapper.getType()))
            .map(SpecRefEntityWrapper::getWorkflow)
            .forEach(workflows::add);

        return workflows.stream().filter(Objects::nonNull).collect(Collectors.toMap(SpecWorkflow::getId, Function.identity(), (v1, v2) -> v1));
    }
}
