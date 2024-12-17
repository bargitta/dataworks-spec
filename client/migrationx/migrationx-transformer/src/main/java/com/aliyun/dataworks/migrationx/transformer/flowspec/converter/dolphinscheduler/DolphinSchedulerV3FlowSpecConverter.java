/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.SpecRefEntity;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.enums.SpecVersion;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.v320.DagDataSchedule;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.FlowSpecConverter;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.common.WorkflowConverter;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.common.context.DolphinSchedulerV3ConverterContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ObjectUtils;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-06-04
 */
@Slf4j
public class DolphinSchedulerV3FlowSpecConverter implements FlowSpecConverter<DagDataSchedule> {

    private static final SpecVersion SPEC_VERSION = SpecVersion.V_1_2_0;

    private final DagDataSchedule dagDataSchedule;

    @Getter
    private final Specification<DataWorksWorkflowSpec> specification;

    private final DolphinSchedulerV3ConverterContext context;

    public DolphinSchedulerV3FlowSpecConverter(DagDataSchedule dagDataSchedule, DolphinSchedulerV3ConverterContext context) {
        this.dagDataSchedule = ObjectUtils.defaultIfNull(dagDataSchedule, new DagDataSchedule());
        this.context = ObjectUtils.defaultIfNull(context, new DolphinSchedulerV3ConverterContext());
        specification = new Specification<>();
        specification.setMetadata(new HashMap<>());
        specification.setSpec(initSpec());
        checkContext();
    }

    private void checkContext() {
        MapUtils.emptyIfNull(context.getDataSourceMap()).values().forEach(datasource -> datasource.setIsRef(false));
        if (Objects.isNull(context.getSpecVersion())) {
            context.setSpecVersion(SPEC_VERSION.getLabel());
        }
    }

    /**
     * init spec instance
     *
     * @return spec
     */
    private DataWorksWorkflowSpec initSpec() {
        DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
        spec.setVariables(new ArrayList<>());
        spec.setTriggers(new ArrayList<>());
        spec.setScripts(new ArrayList<>());
        spec.setFiles(new ArrayList<>());
        spec.setArtifacts(new ArrayList<>());
        spec.setDatasources(new ArrayList<>());
        spec.setDqcRules(new ArrayList<>());
        spec.setRuntimeResources(new ArrayList<>());
        spec.setFileResources(new ArrayList<>());
        spec.setFunctions(new ArrayList<>());
        spec.setNodes(new ArrayList<>());
        spec.setWorkflows(new ArrayList<>());
        spec.setComponents(new ArrayList<>());
        spec.setFlow(new ArrayList<>());

        return spec;
    }

    /**
     * Convert the T type to flowSpec
     *
     * @param from origin obj
     * @return flowSpec result
     */
    @Override
    public List<Specification<DataWorksWorkflowSpec>> convert(DagDataSchedule from) {
        // judge process is manual or cycle workflow
        specification.setKind((Objects.isNull(from.getSchedule()) ? SpecKind.MANUAL_WORKFLOW : SpecKind.CYCLE_WORKFLOW).getLabel());
        specification.setVersion(context.getSpecVersion());

        // convert single process to workflow and add to spec
        log.info("====== start convert ======,version:{}", specification.getVersion());
        doConvert(from);
        log.info("====== finish convert ======");
        return flattenFlowSpec(specification);
    }

    /**
     * convert main method
     *
     * @return flowSpec result
     */
    public List<Specification<DataWorksWorkflowSpec>> convert() {
        return convert(dagDataSchedule);
    }

    /**
     * convert single process to flow spec
     *
     * @param from process
     */
    private void doConvert(DagDataSchedule from) {
        DataWorksWorkflowSpec spec = specification.getSpec();

        SpecWorkflow workflow = new WorkflowConverter(from, context).convert();
        if (CollectionUtils.isEmpty(context.getSubWorkflows())) {
            // normal case
            spec.getWorkflows().add(workflow);
            specification.getMetadata().put("uuid", workflow.getId());
        } else {
            // there are sub processes in dag, add sub process in workflows, other nodes are added in nodes
            spec.getWorkflows().addAll(context.getSubWorkflows());
            spec.getNodes().addAll(workflow.getNodes());
            spec.getFlow().addAll(workflow.getDependencies());
            context.getSubWorkflows().clear();
        }
    }

    /**
     * flatten flow spec when multiple nodes or workflows in a flow spec
     *
     * @param specification flow spec
     * @return flow spec flattened
     */
    private List<Specification<DataWorksWorkflowSpec>> flattenFlowSpec(Specification<DataWorksWorkflowSpec> specification) {
        if (specification == null) {
            return null;
        }
        List<SpecWorkflow> specWorkflows = Optional.ofNullable(specification.getSpec())
            .map(DataWorksWorkflowSpec::getWorkflows)
            .orElse(Collections.emptyList());
        List<SpecNode> specNodes = Optional.ofNullable(specification.getSpec())
            .map(DataWorksWorkflowSpec::getNodes)
            .orElse(Collections.emptyList());

        // need not flatten for single workflow
        if (CollectionUtils.size(specWorkflows) <= 1 && CollectionUtils.isEmpty(specNodes)) {
            return Collections.singletonList(specification);
        }

        // flatten workflow
        List<Specification<DataWorksWorkflowSpec>> flowSpecList = ListUtils.emptyIfNull(specWorkflows).stream()
            .map(specWorkflow -> {
                Specification<DataWorksWorkflowSpec> template = new Specification<>();
                template.setContext(specification.getContext());
                template.setMetadata(new HashMap<>());
                template.setVersion(specification.getVersion());
                template.setKind(specification.getKind());

                template.getMetadata().put("uuid", specWorkflow.getId());

                DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
                template.setSpec(spec);
                spec.setWorkflows(Collections.singletonList(specWorkflow));
                return template;
            })
            .collect(Collectors.toList());

        // flatten node
        List<SpecFlowDepend> specFlowDepends = Optional.ofNullable(specification.getSpec()).map(DataWorksWorkflowSpec::getFlow)
            .orElse(Collections.emptyList());
        List<Specification<DataWorksWorkflowSpec>> nodeSpecList = ListUtils.emptyIfNull(specNodes).stream()
            .map(specNode -> {
                Specification<DataWorksWorkflowSpec> template = new Specification<>();
                template.setContext(specification.getContext());
                template.setMetadata(new HashMap<>());
                template.setVersion(specification.getVersion());
                template.setKind(SpecKind.NODE.getLabel());

                template.getMetadata().put("uuid", specNode.getId());

                DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
                template.setSpec(spec);
                spec.setNodes(Collections.singletonList(specNode));
                List<SpecFlowDepend> specFlowDependsForNode = specFlowDepends.stream()
                    .filter(specFlowDepend -> Optional.ofNullable(specFlowDepend.getNodeId())
                        .map(SpecRefEntity::getId)
                        .filter(id -> id.equals(specNode.getId()))
                        .isPresent())
                    .collect(Collectors.toList());
                spec.setFlow(specFlowDependsForNode);
                return template;
            })
            .collect(Collectors.toList());
        return ListUtils.union(flowSpecList, nodeSpecList);
    }
}
