/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.SpecRefEntity;
import com.aliyun.dataworks.common.spec.domain.enums.ArtifactType;
import com.aliyun.dataworks.common.spec.domain.enums.DependencyType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableScopeType;
import com.aliyun.dataworks.common.spec.domain.interfaces.Input;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.ref.InputOutputWired;
import com.aliyun.dataworks.common.spec.domain.ref.SpecArtifact;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecTable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.ProcessTaskRelation;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.common.context.DolphinSchedulerV3ConverterContext;
import com.aliyun.dataworks.migrationx.transformer.flowspec.model.SpecRefEntityWrapper;
import com.aliyun.migrationx.common.exception.BizException;
import com.aliyun.migrationx.common.exception.ErrorCode;
import com.aliyun.migrationx.common.utils.BeanUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-07-10
 */
@Slf4j
public class SpecFlowDependConverter extends AbstractCommonConverter<List<SpecFlowDepend>> {

    private static final Map<ArtifactType, Class<? extends SpecArtifact>> ARTIFACT_TYPE_CLASS_MAP = new EnumMap<>(ArtifactType.class);

    private final DataWorksWorkflowSpec spec;

    private final SpecWorkflow specWorkflow;

    private final List<ProcessTaskRelation> processTaskRelationList;

    static {
        ARTIFACT_TYPE_CLASS_MAP.put(ArtifactType.TABLE, SpecTable.class);
        ARTIFACT_TYPE_CLASS_MAP.put(ArtifactType.VARIABLE, SpecVariable.class);
        ARTIFACT_TYPE_CLASS_MAP.put(ArtifactType.NODE_OUTPUT, SpecNodeOutput.class);
        ARTIFACT_TYPE_CLASS_MAP.put(ArtifactType.FILE, SpecArtifact.class);
    }

    public SpecFlowDependConverter(DataWorksWorkflowSpec spec, SpecWorkflow specWorkflow, List<ProcessTaskRelation> processTaskRelationList,
                                   DolphinSchedulerV3ConverterContext context) {
        super(context);
        this.spec = spec;
        this.specWorkflow = specWorkflow;
        this.processTaskRelationList = processTaskRelationList;
    }

    /**
     * convert to T type object
     *
     * @return T type object
     */
    @Override
    public List<SpecFlowDepend> convert() {
        if (Objects.nonNull(specWorkflow)) {
            specWorkflow.setDependencies(convertTaskRelationList(processTaskRelationList));
            return specWorkflow.getDependencies();
        }
        spec.setFlow(convertTaskRelationList(processTaskRelationList));
        return spec.getFlow();
    }

    private List<SpecFlowDepend> convertTaskRelationList(List<ProcessTaskRelation> taskRelationList) {
        List<SpecFlowDepend> flow = Optional.ofNullable(specWorkflow).map(SpecWorkflow::getDependencies).orElse(Optional.ofNullable(spec)
            .map(DataWorksWorkflowSpec::getFlow).orElse(new ArrayList<>()));
        Map<String, List<SpecDepend>> nodeIdDependMap = flow.stream().collect(
            Collectors.toMap(o -> o.getNodeId().getId(), SpecFlowDepend::getDepends));
        for (ProcessTaskRelation processTaskRelation : ListUtils.emptyIfNull(taskRelationList)) {
            long preTaskCode = processTaskRelation.getPreTaskCode();
            long postTaskCode = processTaskRelation.getPostTaskCode();
            // The preceding node code of the root node is 0
            if (preTaskCode == 0L) {
                continue;
            }

            List<SpecRefEntityWrapper> preEntityList = context.getEntityTailMap().getOrDefault(preTaskCode, Collections.emptyList());
            List<SpecRefEntityWrapper> postEntityList = context.getEntityHeadMap().getOrDefault(postTaskCode, Collections.emptyList());
            postEntityList.forEach(postNode -> dealSingleNodeDependency(postNode, nodeIdDependMap, flow, preEntityList));
        }
        return flow;
    }

    private void dealSingleNodeDependency(SpecRefEntityWrapper postNode, Map<String, List<SpecDepend>> nodeIdDependMap, List<SpecFlowDepend> flow,
                                          List<SpecRefEntityWrapper> preEntityList) {
        // don't deal with join node because it is dealt when the node is created
        if (Optional.ofNullable(postNode.getNode()).map(SpecNode::getJoin).isPresent()) {
            return;
        }
        List<SpecDepend> specDependList = ListUtils.defaultIfNull(nodeIdDependMap.get(postNode.getId()), new ArrayList<>());
        assert specDependList != null;
        // If the node does not depend on other nodes before, need to create a new dependent node
        if (CollectionUtils.isEmpty(specDependList)) {
            SpecFlowDepend specFlowDepend = newSpecFlowDepend();
            specFlowDepend.setNodeId(postNode.getNode());
            specFlowDepend.setDepends(specDependList);
            // if post node is a workflow, the nodeId will be null, we only need to deal with input and output. don't deal with dependencies
            if (specFlowDepend.getNodeId() != null) {
                flow.add(specFlowDepend);
                nodeIdDependMap.put(postNode.getId(), specDependList);
            }
        }
        setPostNodeDependenciesAndInput(postNode, preEntityList, specDependList);

        transformVariableInput2ScriptVariable(postNode);
    }

    private void setPostNodeDependenciesAndInput(SpecRefEntityWrapper postNode, List<SpecRefEntityWrapper> preEntityList,
                                                 List<SpecDepend> specDependList) {
        // the two sets are used to avoid duplicate dependencies
        Set<String> preEntityIdSet = specDependList.stream()
            .map(SpecDepend::getNodeId)
            .filter(Objects::nonNull)
            .map(SpecRefEntity::getId)
            .collect(Collectors.toSet());

        Set<String> preEntityOutputDataSet = specDependList.stream()
            .map(SpecDepend::getOutput)
            .filter(Objects::nonNull)
            .map(SpecNodeOutput::getData)
            .collect(Collectors.toSet());

        // condition node's dependencies are handled in the previous step
        if (isConditionNode(preEntityIdSet, preEntityList)) {
            log.info("dependencies is dealt in previous step, post node: {}, pre nodes: {}", postNode, preEntityList);
            return;
        }

        for (SpecRefEntityWrapper preEntity : preEntityList) {
            SpecNodeOutput defaultOutput = getDefaultNodeOutput(preEntity);
            if (preEntityIdSet.contains(preEntity.getId())
                || preEntityOutputDataSet.contains(Optional.ofNullable(defaultOutput).map(SpecNodeOutput::getData).orElse(null))) {
                continue;
            }
            preEntityIdSet.add(postNode.getId());
            Optional.ofNullable(defaultOutput).map(SpecNodeOutput::getData).ifPresent(preEntityOutputDataSet::add);
            SpecDepend specDepend = new SpecDepend(preEntity.getNode(), DependencyType.NORMAL, defaultOutput);
            specDependList.add(specDepend);
            postNode.getInputs().addAll(transformOutput2Input(preEntity));
        }
    }

    private boolean isConditionNode(Set<String> preEntityIdSet, List<SpecRefEntityWrapper> preEntityList) {
        if (CollectionUtils.size(preEntityList) != 2) {
            return false;
        }
        return ListUtils.emptyIfNull(preEntityList).stream().map(SpecRefEntityWrapper::getId).filter(preEntityIdSet::contains).count() == 1;
    }

    private void transformVariableInput2ScriptVariable(SpecRefEntityWrapper postNode) {
        // refer context parameter
        List<SpecVariable> scriptParam = Optional.ofNullable(postNode.getScript()).map(SpecScript::getParameters).orElseGet(() -> {
            SpecScript script = postNode.getScript();
            if (script == null) {
                log.error("node.script not set, node id: {}", postNode.getId());
                return new ArrayList<>();
            } else {
                script.setParameters(new ArrayList<>());
                return script.getParameters();
            }
        });
        ListUtils.emptyIfNull(postNode.getInputs()).stream()
            .filter(input -> input instanceof SpecVariable)
            .map(input -> (SpecVariable)input)
            .filter(specVariable -> VariableScopeType.NODE_CONTEXT.equals(specVariable.getScope()))
            .forEach(specVariable -> {
                SpecVariable param = BeanUtils.deepCopy(specVariable, SpecVariable.class);
                param.setId(generateUuid());
                param.setNode(null);
                param.setScope(VariableScopeType.NODE_PARAMETER);
                param.setReferenceVariable(specVariable);
                scriptParam.add(param);
            });
    }

    private List<? extends Input> transformOutput2Input(SpecRefEntityWrapper entity) {
        List<Input> res = new ArrayList<>();
        ListUtils.emptyIfNull(Optional.ofNullable(entity).orElseThrow(() -> new BizException(ErrorCode.PARAMETER_NOT_SET, "spec node"))
            .getOutputs()).forEach(output -> {
                if (output instanceof SpecArtifact) {
                    SpecArtifact specArtifactOutput = (SpecArtifact)output;
                    Class<? extends SpecArtifact> clazz = ARTIFACT_TYPE_CLASS_MAP.get(specArtifactOutput.getArtifactType());
                    // input and output are not same entity, so we need clone it
                    SpecArtifact specArtifactInput = BeanUtils.deepCopy(specArtifactOutput, clazz);
                    res.add(specArtifactInput);
                }
            }
        );
        return res;
    }

    private SpecNodeOutput getDefaultNodeOutput(InputOutputWired inputOutputWired) {
        return ListUtils.emptyIfNull(inputOutputWired.getOutputs()).stream()
            .filter(o -> o instanceof SpecNodeOutput && ((SpecNodeOutput)o).getIsDefault())
            .map(o -> (SpecNodeOutput)o)
            .findFirst().orElse(null);
    }
}
