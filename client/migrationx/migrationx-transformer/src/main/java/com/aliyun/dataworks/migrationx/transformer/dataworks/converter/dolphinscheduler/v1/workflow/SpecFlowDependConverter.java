/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v1.workflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.enums.ArtifactType;
import com.aliyun.dataworks.common.spec.domain.enums.DependencyType;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.ref.SpecArtifact;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecTable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.DolphinSchedulerV2Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.ProcessTaskRelation;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;

@Slf4j
public class SpecFlowDependConverter {

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

    public SpecFlowDependConverter(DataWorksWorkflowSpec spec, SpecWorkflow specWorkflow, List<ProcessTaskRelation> processTaskRelationList) {
        this.spec = spec;
        this.specWorkflow = specWorkflow;
        this.processTaskRelationList = processTaskRelationList;
    }

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

        Map<Long, String> taskCodeNodeDataMap = DolphinSchedulerV2Context.getContext().getTaskCodeNodeDataMap();
        Map<Long, String> taskCodeNodeIdMap = DolphinSchedulerV2Context.getContext().getTaskCodeNodeIdMap();
        for (ProcessTaskRelation processTaskRelation : ListUtils.emptyIfNull(taskRelationList)) {
            long preTaskCode = processTaskRelation.getPreTaskCode();
            if (preTaskCode == 0L) {
                continue;
            }
            long postTaskCode = processTaskRelation.getPostTaskCode();
            String nodeId = taskCodeNodeIdMap.get(postTaskCode);
            SpecNode currentNode;
            if (nodeId == null) {
                log.warn("can not find nodeId {}", postTaskCode);
                continue;
            } else {
                currentNode = specWorkflow.getNodes().stream().filter(node -> nodeId.equals(node.getId()))
                        .findAny().orElse(null);
            }
            final SpecNode finalCurrentNode = currentNode;

            String data = taskCodeNodeDataMap.get(preTaskCode);
            if (data != null) {
                specWorkflow.getNodes().stream()
                        .filter(node -> {
                            return CollectionUtils.emptyIfNull(node.getOutputs())
                                    .stream()
                                    .map(output -> (SpecNodeOutput) output)
                                    .anyMatch(output -> data.equals(output.getData()));
                        })
                        .findAny()
                        .ifPresent(node -> {
                            SpecNodeOutput specNodeOutput = (SpecNodeOutput) node.getOutputs().get(0);
                            SpecDepend specDepend = new SpecDepend(null, DependencyType.NORMAL, specNodeOutput);
                            SpecFlowDepend specFlowDepend = new SpecFlowDepend();
                            specFlowDepend.setDepends(Arrays.asList(specDepend));
                            specFlowDepend.setNodeId(finalCurrentNode);
                            //flow.add(specFlowDepend);
                            specWorkflow.getDependencies().add(specFlowDepend);
                        });
            }
        }
        return flow;
    }
}
