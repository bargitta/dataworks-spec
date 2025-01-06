/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.logic.condition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.dw.codemodel.AbstractBaseCode;
import com.aliyun.dataworks.common.spec.domain.dw.codemodel.ControllerJoinCode;
import com.aliyun.dataworks.common.spec.domain.dw.codemodel.ControllerJoinCode.Status;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.DependencyType;
import com.aliyun.dataworks.common.spec.domain.enums.SpecEntityType;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecJoin;
import com.aliyun.dataworks.common.spec.domain.noref.SpecJoinBranch;
import com.aliyun.dataworks.common.spec.domain.noref.SpecLogic;
import com.aliyun.dataworks.common.spec.domain.ref.InputOutputWired;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.DependentRelation;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.TaskExecutionStatus;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.DependentItem;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.DependentTaskModel;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.v320.task.condition.ConditionsParameters;
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
public class ConditionsParameterConverter extends AbstractParameterConverter<ConditionsParameters> {

    private static final SpecScriptRuntime RUNTIME = new SpecScriptRuntime();

    static {
        RUNTIME.setEngine(CodeProgramType.CONTROLLER_JOIN.getCalcEngineType().getLabel());
        RUNTIME.setCommand(CodeProgramType.CONTROLLER_JOIN.getName());
    }

    public ConditionsParameterConverter(DataWorksWorkflowSpec spec, SpecWorkflow specWorkflow, TaskDefinition taskDefinition,
                                        DolphinSchedulerV3ConverterContext context) {
        super(spec, specWorkflow, taskDefinition, context);
    }

    /**
     * Each node translates the specific logic of the parameters
     */
    @Override
    protected void convertParameter(SpecNode finalJoinNode) {
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

        // build branch, which is finalJoinNode's downstream
        SpecNode successNode = buildBranchNode(finalJoinNode, Status.SUCCESS, "_success");
        SpecNode failNode = buildBranchNode(finalJoinNode, Status.FAILURE, "_fail");

        Optional.ofNullable(parameter).map(ConditionsParameters::getConditionResult)
            .ifPresent(conditionResult -> {
                ListUtils.emptyIfNull(conditionResult.getSuccessNode()).stream()
                    .filter(Objects::nonNull)
                    .forEach(code -> addBranchRelation(successNode, code));
                ListUtils.emptyIfNull(conditionResult.getFailedNode()).stream()
                    .filter(Objects::nonNull)
                    .forEach(code -> addBranchRelation(failNode, code));
            });

        tailList.add(newWrapper(successNode));
        tailList.add(newWrapper(failNode));

        // build finalJoinNode's upstream
        Optional.ofNullable(parameter).map(ConditionsParameters::getDependence).ifPresent(dependence -> {
            List<SpecNode> nodeList = new ArrayList<>();
            for (int i = 0; i < ListUtils.emptyIfNull(dependence.getDependTaskList()).size(); i++) {
                DependentTaskModel dependentTaskModel = dependence.getDependTaskList().get(i);
                List<SpecJoinBranch> joinBranchList = ListUtils.emptyIfNull(dependentTaskModel.getDependItemList()).stream()
                    .map(this::buildSpecJoinBranch)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
                SpecJoin specJoin = newSpecJoin(joinBranchList, dependentTaskModel.getRelation());
                // we can reduce a layer when the size of dependTaskList is 1
                SpecNode specNode = dependence.getDependTaskList().size() <= 1 ?
                    finalJoinNode : copyJoinNode(finalJoinNode, specJoin, "_join_" + i);

                // if spec node is final join node, then set join and build script content
                if (specNode.getJoin() == null) {
                    specNode.setJoin(specJoin);
                    Optional.ofNullable(specNode.getScript()).ifPresent(s -> s.setContent(buildControllerJoinCode(specNode).getContent()));
                }

                List<SpecNode> subNodeList = ListUtils.emptyIfNull(joinBranchList).stream()
                    .map(SpecJoinBranch::getNodeId)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

                List<SpecNodeOutput> subNodeOutputList = ListUtils.emptyIfNull(joinBranchList).stream()
                    .filter(joinBranch -> Objects.isNull(joinBranch.getNodeId()))
                    .map(SpecJoinBranch::getOutput)
                    .collect(Collectors.toList());
                addRelation(specNode, subNodeList, subNodeOutputList);
                nodeList.add(specNode);
            }
            // join node not in node list, join node is downstream of nodes in nodeList
            if (CollectionUtils.size(nodeList) > 1) {
                List<SpecJoinBranch> specJoinBranchList = ListUtils.emptyIfNull(nodeList).stream()
                    .map(node -> buildSpecJoinBranch(node, Status.SUCCESS))
                    .collect(Collectors.toList());
                SpecJoin specJoin = newSpecJoin(specJoinBranchList, dependence.getRelation());
                finalJoinNode.setJoin(specJoin);
                // build controller join code
                Optional.ofNullable(buildControllerJoinCode(finalJoinNode))
                    .map(AbstractBaseCode::getContent)
                    .ifPresent(script::setContent);
                addRelation(finalJoinNode, nodeList);
            }
        });
    }

    private void addBranchRelation(SpecNode preNode, Long taskCode) {
        List<SpecRefEntityWrapper> specRefEntityWrappers = context.getEntityHeadMap().get(taskCode);
        ListUtils.emptyIfNull(specRefEntityWrappers)
            .forEach(entity -> {
                if (SpecEntityType.NODE.equals(entity.getType())) {
                    SpecFlowDepend specFlowDepend = newSpecFlowDepend();
                    specFlowDepend.setNodeId(entity.getNode());
                    specFlowDepend.getDepends().add(new SpecDepend(preNode, DependencyType.NORMAL, getDefaultOutput(preNode)));
                    getWorkflowDependencyList().add(specFlowDepend);
                }
                InputOutputWired inputOutputWired = entity.getAsInputOutputWired();
                inputOutputWired.getInputs().add(getDefaultOutput(preNode));
            });
    }

    private SpecNode buildBranchNode(SpecNode finalJoinNode, Status status, String suffix) {
        SpecJoinBranch branch = buildSpecJoinBranch(finalJoinNode, status);
        SpecJoin specJoin = newSpecJoin(Collections.singletonList(branch), DependentRelation.AND);
        SpecNode specNode = copyJoinNode(finalJoinNode, specJoin, suffix);
        addRelation(specNode, Collections.singletonList(finalJoinNode), null);
        return specNode;
    }

    private List<SpecJoinBranch> buildSpecJoinBranch(DependentItem dependentItem) {
        ControllerJoinCode.Status status =
            dependentItem.getStatus() == TaskExecutionStatus.FAILURE ? ControllerJoinCode.Status.FAILURE : ControllerJoinCode.Status.SUCCESS;
        List<SpecRefEntityWrapper> specRefEntityWrappers = context.getEntityTailMap().get(dependentItem.getDepTaskCode());
        // build spec join branch with node
        List<SpecJoinBranch> specJoinBranchList = ListUtils.emptyIfNull(specRefEntityWrappers).stream()
            .map(SpecRefEntityWrapper::getNode)
            .filter(Objects::nonNull)
            .map(node -> buildSpecJoinBranch(node, status))
            .collect(Collectors.toList());
        // build spec join branch with output of workflow
        List<SpecJoinBranch> specJoinBranchListWithOutput = ListUtils.emptyIfNull(specRefEntityWrappers).stream()
            .filter(entity -> entity.getNode() == null)
            .map(SpecRefEntityWrapper::getAsInputOutputWired)
            .filter(Objects::nonNull)
            .map(InputOutputWired::getOutputs)
            .map(this::getDefaultOutput)
            .map(output -> buildSpecJoinBranch(output, status))
            .collect(Collectors.toList());
        // merge them and return
        return ListUtils.union(specJoinBranchList, specJoinBranchListWithOutput);
    }

    protected SpecJoin newSpecJoin(List<SpecJoinBranch> joinBranchList, DependentRelation relation) {
        SpecJoin specJoin = new SpecJoin();
        specJoin.setBranches(joinBranchList);
        List<String> branchNameList = ListUtils.emptyIfNull(joinBranchList).stream()
            .map(SpecJoinBranch::getName)
            .collect(Collectors.toList());
        SpecLogic specLogic = new SpecLogic();
        specLogic.setExpression(String.join(" " + relation.name() + " ", branchNameList));
        specJoin.setLogic(specLogic);
        specJoin.setResultStatus(Status.SUCCESS.getCode());
        return specJoin;
    }

}
