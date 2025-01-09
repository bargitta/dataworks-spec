/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.enums.DependencyType;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.interfaces.Output;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecSubFlow;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.DolphinSchedulerPackage;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.Project;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DolphinSchedulerV3Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.DataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.ResourceComponent;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.UdfFunc;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

@Slf4j
public class DolphinSchedulerV3WorkflowConverter {
    public static final String SPEC_VERSION = "1.2.0";

    private DolphinSchedulerPackage<Project, DagData,
            DataSource, ResourceComponent, UdfFunc> dolphinSchedulerPackage;

    private final List<DagData> dagDataList;
    private final Properties converterProperties;

    public DolphinSchedulerV3WorkflowConverter(DolphinSchedulerPackage<Project, DagData, DataSource, ResourceComponent, UdfFunc> dolphinSchedulerPackage,
            Properties converterProperties) {
        this.dolphinSchedulerPackage = dolphinSchedulerPackage;
        this.converterProperties = converterProperties;
        this.dagDataList = dolphinSchedulerPackage.getProcessDefinitions().values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
        if (dagDataList.isEmpty()) {
            throw new RuntimeException("process list empty");
        }
    }

    public List<Specification<DataWorksWorkflowSpec>> convert() {
        List<Specification<DataWorksWorkflowSpec>> specifications = new ArrayList<>();
        for (DagData dagData : dagDataList) {
            List<SpecWorkflow> workflows = new ArrayList<>();
            //convert process to workflow
            Specification<DataWorksWorkflowSpec> specification = new Specification<>();
            specification.setKind(SpecKind.CYCLE_WORKFLOW.getLabel());
            specification.setVersion(SPEC_VERSION);
            DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
            String processName = dagData.getProcessDefinition().getName();
            spec.setName(processName);
            V3ProcessDefinitionConverter converter = new V3ProcessDefinitionConverter(dagData, this.converterProperties);
            SpecWorkflow workflow = converter.convert();
            workflows.add(workflow);
            spec.setWorkflows(workflows);
            specification.setSpec(spec);
            specifications.add(specification);
        }
        handleSubprocess();
        handleDependents(specifications);
        return specifications;
    }

    /**
     * subprocess
     */
    private void handleSubprocess() {
        Map<Long, Object> codeWorkflowMap = DolphinSchedulerV3Context.getContext().getSubProcessCodeWorkflowMap();
        Map<Long, Object> codeNodeMap = DolphinSchedulerV3Context.getContext().getSubProcessCodeNodeMap();
        //find subprocess
        for (Map.Entry<Long, Object> entry : codeNodeMap.entrySet()) {
            //find workflow
            SpecWorkflow specWorkflow = (SpecWorkflow) codeWorkflowMap.get(entry.getKey());
            SpecNodeOutput specNodeOutput;
            if (specWorkflow.getOutputs().isEmpty()) {
                specNodeOutput = new SpecNodeOutput();
                specNodeOutput.setData(specWorkflow.getId());
                specWorkflow.getOutputs().add(specNodeOutput);
            } else {
                specNodeOutput = (SpecNodeOutput) specWorkflow.getOutputs().get(0);
            }

            SpecNode subprocess = (SpecNode) entry.getValue();
            SpecSubFlow subflow = new SpecSubFlow();
            subflow.setOutput(specNodeOutput.getData());
            subprocess.setSubflow(subflow);
        }
    }

    /**
     * dependent process
     *
     * @param specifications
     */
    private void handleDependents(List<Specification<DataWorksWorkflowSpec>> specifications) {
        Map<String, SpecWorkflow> nodeIdWorkflowMap = new HashMap<>();
        for (Specification<DataWorksWorkflowSpec> specification : specifications) {
            specification.getSpec().getWorkflows().forEach(workflow -> {
                workflow.getNodes().forEach(node -> {
                    nodeIdWorkflowMap.put(node.getId(), workflow);
                });
            });
        }
        Map<Long, Object> taskCodeSpecNodeMap = DolphinSchedulerV3Context.getContext().getTaskCodeSpecNodeMap();
        Map<Object, List<Long>> depMap = DolphinSchedulerV3Context.getContext().getSpecNodeProcessCodeMap();
        for (Map.Entry<Object, List<Long>> entry : depMap.entrySet()) {
            SpecNode specNode = (SpecNode) entry.getKey();
            SpecWorkflow workflow = nodeIdWorkflowMap.get(specNode.getId());
            SpecFlowDepend specFlowDepend = new SpecFlowDepend();

            List<Long> dependents = entry.getValue();
            List<SpecDepend> depends = new ArrayList<>();
            for (Long code : dependents) {
                SpecNode depNode = (SpecNode) taskCodeSpecNodeMap.get(code);
                if (depNode == null) {
                    log.warn("can not find spec node: {}", code);
                    continue;
                }
                SpecNodeOutput specNodeOutput = new SpecNodeOutput();
                List<Output> outputs = depNode.getOutputs();
                String data = depNode.getId();
                if (CollectionUtils.isNotEmpty(outputs)) {
                    data = ((SpecNodeOutput) outputs.get(0)).getData();
                }
                specNodeOutput.setData(data);
                SpecDepend specDepend = new SpecDepend(null, DependencyType.NORMAL, specNodeOutput);
                depends.add(specDepend);
            }
            specFlowDepend.setDepends(depends);
            specFlowDepend.setNodeId(specNode);
            workflow.getDependencies().add(specFlowDepend);
        }
        mergeDeps(nodeIdWorkflowMap);
    }

    private void mergeDeps(Map<String, SpecWorkflow> nodeIdWorkflowMap) {
        nodeIdWorkflowMap.values().stream().forEach(workflow -> {
            List<SpecFlowDepend> depends = workflow.getDependencies();

            if (CollectionUtils.isNotEmpty(depends)) {
                Map<SpecNode, List<SpecDepend>> map = new HashMap<>();
                depends.stream().forEach(dep -> {
                    List<SpecDepend> specNodes = map.get(dep.getNodeId());
                    if (CollectionUtils.isNotEmpty(specNodes)) {
                        specNodes.addAll(dep.getDepends());
                    } else {
                        specNodes = new ArrayList<>();
                        specNodes.addAll(dep.getDepends());
                        map.put(dep.getNodeId(), specNodes);
                    }
                });
                List<SpecFlowDepend> specFlowDepends = new ArrayList<>();
                for (Map.Entry<SpecNode, List<SpecDepend>> entry : map.entrySet()) {
                    SpecFlowDepend specFlowDepend = new SpecFlowDepend();
                    specFlowDepend.setNodeId(entry.getKey());
                    specFlowDepend.setDepends(entry.getValue());
                    specFlowDepends.add(specFlowDepend);
                }
                workflow.setDependencies(specFlowDepends);
            }
        });
    }
}
