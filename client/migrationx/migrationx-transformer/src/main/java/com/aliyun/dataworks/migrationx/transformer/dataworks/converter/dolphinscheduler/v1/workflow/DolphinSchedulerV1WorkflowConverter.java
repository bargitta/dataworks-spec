/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v1.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.DependencyType;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecSubFlow;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.DolphinSchedulerPackage;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.Project;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.DolphinSchedulerV1Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.ProcessData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.ProcessMeta;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.UdfFunc;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.datasource.DataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.entity.ResourceInfo;

public class DolphinSchedulerV1WorkflowConverter {
    public static final String SPEC_VERSION = "1.2.0";

    protected List<ProcessMeta> processMetaList;
    protected Map<String, CodeProgramType> nodeTypeMap = new HashMap<>();
    private DolphinSchedulerPackage<Project, ProcessMeta,
            DataSource, ResourceInfo, UdfFunc>
            dolphinSchedulerPackage;

    private final Properties converterProperties;

    public DolphinSchedulerV1WorkflowConverter(DolphinSchedulerPackage dolphinSchedulerPackage, Properties converterProperties) {
        this.dolphinSchedulerPackage = dolphinSchedulerPackage;
        this.converterProperties = converterProperties;
    }

    public List<Specification<DataWorksWorkflowSpec>> convert() {
        processMetaList = dolphinSchedulerPackage.getProcessDefinitions().values()
                .stream().flatMap(List::stream).collect(Collectors.toList());

        List<Specification<DataWorksWorkflowSpec>> specifications = new ArrayList<>();
        for (ProcessMeta processMeta : processMetaList) {
            List<SpecWorkflow> workflows = new ArrayList<>();
            //convert process to workflow
            Specification<DataWorksWorkflowSpec> specification = new Specification<>();
            specification.setKind(SpecKind.CYCLE_WORKFLOW.getLabel());
            specification.setVersion(SPEC_VERSION);
            DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
            //todo spec name
            String processName = processMeta.getProcessDefinitionName();
            spec.setName(processName);
            ProcessData processData = processMeta.getProcessDefinitionJson();
            V1ProcessDefinitionConverter converter = new V1ProcessDefinitionConverter(processMeta, processData.getTasks(), this.converterProperties);
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
        Map<Integer, Object> codeWorkflowMap = DolphinSchedulerV1Context.getContext().getSubProcessCodeWorkflowMap();
        Map<Integer, Object> codeNodeMap = DolphinSchedulerV1Context.getContext().getSubProcessCodeNodeMap();
        //find subprocess
        for (Map.Entry<Integer, Object> entry : codeNodeMap.entrySet()) {
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
        Map<String, Object> taskCodeSpecNodeMap = DolphinSchedulerV1Context.getContext().getTaskCodeSpecNodeMap();
        Map<Object, List<String>> depMap = DolphinSchedulerV1Context.getContext().getSpecNodeProcessCodeMap();

        for (Map.Entry<Object, List<String>> entry : depMap.entrySet()) {
            SpecNode specNode = (SpecNode) entry.getKey();
            SpecWorkflow workflow = nodeIdWorkflowMap.get(specNode.getId());
            SpecFlowDepend specFlowDepend = new SpecFlowDepend();

            List<String> dependents = entry.getValue();
            List<SpecDepend> depends = new ArrayList<>();
            for (String id : dependents) {
                SpecNode depNode = (SpecNode) taskCodeSpecNodeMap.get(id);
                SpecNodeOutput specNodeOutput = new SpecNodeOutput();
                specNodeOutput.setData(depNode.getId());
                SpecDepend specDepend = new SpecDepend(null, DependencyType.NORMAL, specNodeOutput);
                depends.add(specDepend);
            }
            specFlowDepend.setDepends(depends);

            specFlowDepend.setNodeId(specNode);
            workflow.getDependencies().add(specFlowDepend);
        }
    }
}
