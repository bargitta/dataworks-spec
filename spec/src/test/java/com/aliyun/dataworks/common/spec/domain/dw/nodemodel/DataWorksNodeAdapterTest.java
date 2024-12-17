/*
 * Copyright (c) 2024, Alibaba Cloud;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.dataworks.common.spec.domain.dw.nodemodel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONWriter.Feature;

import com.aliyun.dataworks.common.spec.SpecUtil;
import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.dw.nodemodel.DataWorksNodeAdapter.Context;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.DependencyType;
import com.aliyun.dataworks.common.spec.domain.enums.NodeRecurrenceType;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScheduleStrategy;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.domain.ref.component.SpecComponent;
import com.aliyun.dataworks.common.spec.domain.ref.component.SpecComponentParameter;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.common.spec.writer.SpecWriterContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author 聿剑
 * @date 2023/11/10
 */
@Slf4j
public class DataWorksNodeAdapterTest {
    @Test
    public void test1() throws IOException {
        String spec = IOUtils.toString(
            Objects.requireNonNull(DataWorksNodeAdapterTest.class.getClassLoader().getResource("nodemodel/assignment.json")),
            StandardCharsets.UTF_8);

        System.out.println(spec);
        Specification<DataWorksWorkflowSpec> specObj = SpecUtil.parseToDomain(spec);
        DataWorksWorkflowSpec specification = specObj.getSpec();
        Assert.assertNotNull(specification);

        Assert.assertNotNull(specification.getNodes());
        SpecNode node = specification.getNodes().get(0);

        DataWorksNodeAdapter adapter = new DataWorksNodeAdapter(specObj, node);
        System.out.println("code: " + adapter.getCode());
        Assert.assertNotNull(adapter.getCode());
        Assert.assertTrue(adapter.getCode().contains("language"));

        System.out.println("inputs: " + adapter.getInputs());
        System.out.println("outputs: " + adapter.getOutputs());

        Assert.assertTrue(CollectionUtils.isNotEmpty(adapter.getInputs()));
        Assert.assertEquals(2, CollectionUtils.size(adapter.getInputs()));
        Assert.assertTrue(CollectionUtils.isNotEmpty(adapter.getOutputs()));
        Assert.assertEquals(1, CollectionUtils.size(adapter.getOutputs()));

        System.out.println("context inputs: " + adapter.getInputContexts());
        System.out.println("context outputs: " + adapter.getOutputContexts());
        Assert.assertTrue(CollectionUtils.isNotEmpty(adapter.getInputContexts()));
        Assert.assertTrue(CollectionUtils.isNotEmpty(adapter.getOutputContexts()));
    }

    @Test
    public void testGetDependentType() throws IOException {
        String spec = IOUtils.toString(
            Objects.requireNonNull(DataWorksNodeAdapterTest.class.getClassLoader().getResource("nodemodel/all_depend_types.json")),
            StandardCharsets.UTF_8);

        System.out.println(spec);
        Specification<DataWorksWorkflowSpec> specObj = SpecUtil.parseToDomain(spec);
        Assert.assertNotNull(specObj);

        DataWorksWorkflowSpec specification = specObj.getSpec();
        Assert.assertNotNull(specification);
        Assert.assertEquals(1, CollectionUtils.size(specification.getNodes()));

        DwNodeDependentTypeInfo depInfo = getDwNodeDependentTypeInfo(specObj);
        System.out.println("depInfo: {}" + depInfo);
        Assert.assertNotNull(depInfo);
        Assert.assertEquals(DwNodeDependentTypeInfo.USER_DEFINE_AND_SELF, depInfo.getDependentType());
        Assert.assertEquals(3, CollectionUtils.size(depInfo.getDependentNodeIdList()));

        DataWorksNodeAdapter adapter = new DataWorksNodeAdapter(specObj, specObj.getSpec().getNodes().get(0));
        Assert.assertNotNull(adapter.getInputs());
        Assert.assertTrue(adapter.getInputs().stream().filter(in -> in instanceof SpecNodeOutput)
            .anyMatch(in -> StringUtils.equals("test_node_1", ((SpecNodeOutput)in).getRefTableName())));
    }

    private DwNodeDependentTypeInfo getDwNodeDependentTypeInfo(Specification<DataWorksWorkflowSpec> specification) {
        DataWorksNodeAdapter adapter = new DataWorksNodeAdapter(specification, specification.getSpec().getNodes().get(0));
        return adapter.getDependentType((specNodeOutputs) -> {
            System.out.println(ListUtils.emptyIfNull(specNodeOutputs).stream().map(SpecNodeOutput::getData).collect(Collectors.toList()));
            return ListUtils.emptyIfNull(specNodeOutputs).stream().map(SpecNodeOutput::getData)
                .map(String::hashCode)
                .map(Math::abs)
                .map(Long::valueOf)
                .collect(Collectors.toList());
        });
    }

    @Test
    public void testDowhile() throws IOException {
        String spec = IOUtils.toString(
            Objects.requireNonNull(DataWorksNodeAdapterTest.class.getClassLoader().getResource("nodemodel/dowhile.json")),
            StandardCharsets.UTF_8);

        System.out.println(spec);
        Specification<DataWorksWorkflowSpec> specObj = SpecUtil.parseToDomain(spec);
        Assert.assertNotNull(specObj);

        DataWorksWorkflowSpec specification = specObj.getSpec();
        Assert.assertNotNull(specification);
        Assert.assertEquals(1, CollectionUtils.size(specification.getNodes()));

        SpecNode dowhile = specification.getNodes().get(0);
        Assert.assertNotNull(dowhile);

        Assert.assertNotNull(dowhile.getDoWhile());
        Assert.assertNotNull(dowhile.getDoWhile().getSpecWhile());
        Assert.assertNotNull(dowhile.getDoWhile().getNodes());
        Assert.assertEquals(4, (int)dowhile.getDoWhile().getMaxIterations());
        DataWorksNodeAdapter dataWorksNodeAdapter = new DataWorksNodeAdapter(specObj, dowhile.getDoWhile().getSpecWhile());
        System.out.println(dataWorksNodeAdapter.getCode());
        System.out.println(dataWorksNodeAdapter.getInputs());
        System.out.println(JSON.toJSONString(SpecUtil.write(dowhile, new SpecWriterContext()), Feature.PrettyFormat));

        DataWorksNodeAdapter dowhileAdapter = new DataWorksNodeAdapter(specObj, dowhile);
        Map<String, Object> extConfig = dowhileAdapter.getExtConfig();
        Assert.assertNotNull(extConfig);
        Assert.assertEquals(4, (int)extConfig.get(DataWorksNodeAdapter.LOOP_COUNT));
    }

    @Test
    public void testForeach() throws IOException {
        String spec = IOUtils.toString(
            Objects.requireNonNull(DataWorksNodeAdapterTest.class.getClassLoader().getResource("nodemodel/foreach.json")),
            StandardCharsets.UTF_8);

        System.out.println(spec);
        Specification<DataWorksWorkflowSpec> specObj = SpecUtil.parseToDomain(spec);
        Assert.assertNotNull(specObj);
        System.out.println(SpecUtil.writeToSpec(specObj));

        DataWorksWorkflowSpec specification = specObj.getSpec();
        Assert.assertNotNull(specification);
        Assert.assertEquals(1, CollectionUtils.size(specification.getNodes()));

        SpecNode foreach = specification.getNodes().get(0);
        Assert.assertNotNull(foreach);

        Assert.assertNotNull(foreach.getForeach());
        Assert.assertNotNull(foreach.getForeach().getNodes());
        Assert.assertEquals(3, CollectionUtils.size(foreach.getInnerNodes()));
        Assert.assertNotNull(foreach.getInnerDependencies());

        ListUtils.emptyIfNull(specObj.getSpec().getNodes().get(0).getInnerNodes()).forEach(inner -> {
            DataWorksNodeAdapter adapter = new DataWorksNodeAdapter(specObj, inner);
            log.info("name: {}, inputs: {}, outputs: {}", inner.getName(), adapter.getInputs(), adapter.getOutputs());
            Assert.assertTrue(CollectionUtils.isNotEmpty(adapter.getOutputs()));
            Assert.assertEquals(inner.getId(), ((SpecNodeOutput)adapter.getOutputs().get(0)).getData());
        });
    }

    @Test
    public void testShell() throws IOException {
        String spec = IOUtils.toString(
            Objects.requireNonNull(DataWorksNodeAdapterTest.class.getClassLoader().getResource("nodemodel/dide_shell.json")),
            StandardCharsets.UTF_8);

        System.out.println(spec);
        Specification<DataWorksWorkflowSpec> specObj = SpecUtil.parseToDomain(spec);
        Assert.assertNotNull(specObj);

        DataWorksWorkflowSpec specification = specObj.getSpec();
        Assert.assertNotNull(specification);
        Assert.assertEquals(1, CollectionUtils.size(specification.getNodes()));

        SpecNode shellNode = specification.getNodes().get(0);
        Assert.assertNotNull(shellNode);

        DataWorksNodeAdapter adapter = new DataWorksNodeAdapter(specObj, shellNode);
        log.info("para value: {}", adapter.getParaValue());
        Assert.assertNotNull(adapter.getParaValue());
        Assert.assertEquals("111111 222222", adapter.getParaValue());
        Assert.assertEquals(3, (int)adapter.getNodeType());
    }

    @Test
    public void testPyOdps2() throws IOException {
        String spec = IOUtils.toString(
            Objects.requireNonNull(DataWorksNodeAdapterTest.class.getClassLoader().getResource("nodemodel/pyodps2.json")),
            StandardCharsets.UTF_8);

        System.out.println(spec);
        Specification<DataWorksWorkflowSpec> specObj = SpecUtil.parseToDomain(spec);
        Assert.assertNotNull(specObj);

        DataWorksWorkflowSpec specification = specObj.getSpec();
        Assert.assertNotNull(specification);
        Assert.assertEquals(1, CollectionUtils.size(specification.getNodes()));

        SpecNode shellNode = specification.getNodes().get(0);
        Assert.assertNotNull(shellNode);

        shellNode.setIgnoreBranchConditionSkip(true);
        shellNode.setTimeout(0);

        DataWorksNodeAdapter adapter = new DataWorksNodeAdapter(specObj, shellNode);
        log.info("para value: {}", adapter.getParaValue());
        Assert.assertNotNull(adapter.getParaValue());
        Assert.assertEquals("2=222222 1=111111", adapter.getParaValue());

        log.info("extConfig: {}", adapter.getExtConfig());
        Assert.assertNotNull(adapter.getExtConfig());
        Assert.assertTrue(adapter.getExtConfig().containsKey(DataWorksNodeAdapter.IGNORE_BRANCH_CONDITION_SKIP));
        Assert.assertFalse(adapter.getExtConfig().containsKey(DataWorksNodeAdapter.TIMEOUT));

        Assert.assertEquals(0, (int)adapter.getNodeType());
    }

    @Test
    public void testGetDependentTypeWithOutputs() {
        Specification<DataWorksWorkflowSpec> spec = new Specification<>();
        DataWorksWorkflowSpec dwSpec = new DataWorksWorkflowSpec();
        SpecFlowDepend flow = new SpecFlowDepend();
        SpecNode nodeId = new SpecNode();
        nodeId.setId("1");
        flow.setNodeId(nodeId);
        SpecDepend dep = new SpecDepend();
        dep.setType(DependencyType.CROSS_CYCLE_OTHER_NODE);
        SpecNodeOutput out = new SpecNodeOutput();
        out.setData("output1");
        dep.setOutput(out);
        flow.setDepends(Collections.singletonList(dep));
        dwSpec.setFlow(Collections.singletonList(flow));
        spec.setSpec(dwSpec);
        SpecNode node = new SpecNode();
        node.setId("1");
        node.setRecurrence(NodeRecurrenceType.PAUSE);
        DataWorksNodeAdapter dataWorksNodeAdapter = new DataWorksNodeAdapter(spec, node);
        DwNodeDependentTypeInfo info = dataWorksNodeAdapter.getDependentType(null);
        Assert.assertNotNull(info);
        Assert.assertEquals(info.getDependentType(), DwNodeDependentTypeInfo.USER_DEFINE);
        Assert.assertNotNull(info.getDependentNodeOutputList());
        Assert.assertTrue(info.getDependentNodeOutputList().contains("output1"));

        Assert.assertEquals(2, (int)dataWorksNodeAdapter.getNodeType());
    }

    @Test
    public void testGetDependentTypeForWorkflowWithOutputs() {
        String specStr = "{\n"
            + "\t\"version\":\"1.1.0\",\n"
            + "\t\"kind\":\"CycleWorkflow\",\n"
            + "\t\"spec\":{\n"
            + "\t\t\"name\":\"工作流跨周期小时依赖内部节点依赖外部\",\n"
            + "\t\t\"id\":\"5992975956233455901\",\n"
            + "\t\t\"type\":\"CycleWorkflow\",\n"
            + "\t\t\"owner\":\"206561090452322657\",\n"
            + "\t\t\"workflows\":[\n"
            + "\t\t\t{\n"
            + "\t\t\t\t\"script\":{\n"
            + "\t\t\t\t\t\"path\":\"李文涛测试工作流/工作流调度依赖跨周期场景/工作流跨周期小时依赖内部节点依赖外部\",\n"
            + "\t\t\t\t\t\"runtime\":{\n"
            + "\t\t\t\t\t\t\"command\":\"WORKFLOW\"\n"
            + "\t\t\t\t\t},\n"
            + "\t\t\t\t\t\"id\":\"4928814091066534424\"\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t\"id\":\"5992975956233455901\",\n"
            + "\t\t\t\t\"trigger\":{\n"
            + "\t\t\t\t\t\"type\":\"Scheduler\",\n"
            + "\t\t\t\t\t\"id\":\"7019189558026663217\",\n"
            + "\t\t\t\t\t\"cron\":\"00 13 00 * * ?\",\n"
            + "\t\t\t\t\t\"startTime\":\"1970-01-01 00:00:00\",\n"
            + "\t\t\t\t\t\"endTime\":\"9999-01-01 00:00:00\",\n"
            + "\t\t\t\t\t\"timezone\":\"Asia/Shanghai\",\n"
            + "\t\t\t\t\t\"delaySeconds\":0\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t\"strategy\":{\n"
            + "\t\t\t\t\t\"timeout\":0,\n"
            + "\t\t\t\t\t\"rerunTimes\":3,\n"
            + "\t\t\t\t\t\"rerunInterval\":180000,\n"
            + "\t\t\t\t\t\"failureStrategy\":\"Break\"\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t\"name\":\"工作流跨周期小时依赖内部节点依赖外部\",\n"
            + "\t\t\t\t\"owner\":\"206561090452322657\",\n"
            + "\t\t\t\t\"metadata\":{\n"
            + "\t\t\t\t\t\"owner\":\"206561090452322657\",\n"
            + "\t\t\t\t\t\"tenantId\":\"524257424564736\",\n"
            + "\t\t\t\t\t\"project\":{\n"
            + "\t\t\t\t\t\t\"mode\":\"SIMPLE\",\n"
            + "\t\t\t\t\t\t\"projectOwnerId\":\"1107550004253538\",\n"
            + "\t\t\t\t\t\t\"tenantId\":\"524257424564736\",\n"
            + "\t\t\t\t\t\t\"simple\":true,\n"
            + "\t\t\t\t\t\t\"projectIdentifier\":\"lwt_test_newIde\",\n"
            + "\t\t\t\t\t\t\"projectName\":\"李文涛测试新版ide\",\n"
            + "\t\t\t\t\t\t\"projectId\":\"528891\"\n"
            + "\t\t\t\t\t},\n"
            + "\t\t\t\t\t\"ownerName\":\"lwttest04\",\n"
            + "\t\t\t\t\t\"projectId\":\"528891\",\n"
            + "\t\t\t\t\t\"schedulerNodeId\":700006657376\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t\"inputs\":{\n"
            + "\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t\"outputs\":{\n"
            + "\t\t\t\t\t\"nodeOutputs\":[\n"
            + "\t\t\t\t\t\t{\n"
            + "\t\t\t\t\t\t\t\"data\":\"5992975956233455901\",\n"
            + "\t\t\t\t\t\t\t\"artifactType\":\"NodeOutput\",\n"
            + "\t\t\t\t\t\t\t\"refTableName\":\"工作流跨周期小时依赖内部节点依赖外部\",\n"
            + "\t\t\t\t\t\t\t\"isDefault\":true\n"
            + "\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t]\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t\"nodes\":[\n"
            + "\t\t\t\t\t{\n"
            + "\t\t\t\t\t\t\"recurrence\":\"Normal\",\n"
            + "\t\t\t\t\t\t\"id\":\"6203019234746940306\",\n"
            + "\t\t\t\t\t\t\"timeout\":0,\n"
            + "\t\t\t\t\t\t\"instanceMode\":\"T+1\",\n"
            + "\t\t\t\t\t\t\"rerunMode\":\"Allowed\",\n"
            + "\t\t\t\t\t\t\"rerunTimes\":3,\n"
            + "\t\t\t\t\t\t\"rerunInterval\":180000,\n"
            + "\t\t\t\t\t\t\"script\":{\n"
            + "\t\t\t\t\t\t\t\"path\":\"李文涛测试工作流/工作流调度依赖跨周期场景/工作流跨周期小时依赖内部节点依赖外部/工作流跨周期小时依赖内部节点依赖外部_内部节点\",\n"
            + "\t\t\t\t\t\t\t\"runtime\":{\n"
            + "\t\t\t\t\t\t\t\t\"command\":\"DIDE_SHELL\",\n"
            + "\t\t\t\t\t\t\t\t\"commandTypeId\":6\n"
            + "\t\t\t\t\t\t\t},\n"
            + "\t\t\t\t\t\t\t\"content\":\"#!/bin/bash\\n#********************************************************************#\\n##author"
            + ":lwttest04\\n##create time:2024-08-19 18:41:26\\n#********************************************************************#\",\n"
            + "\t\t\t\t\t\t\t\"id\":\"8296284929718477332\"\n"
            + "\t\t\t\t\t\t},\n"
            + "\t\t\t\t\t\t\"trigger\":{\n"
            + "\t\t\t\t\t\t\t\"type\":\"Scheduler\",\n"
            + "\t\t\t\t\t\t\t\"id\":\"4696080677296779430\",\n"
            + "\t\t\t\t\t\t\t\"cron\":\"00 06 00 * * ?\",\n"
            + "\t\t\t\t\t\t\t\"startTime\":\"1970-01-01 00:00:00\",\n"
            + "\t\t\t\t\t\t\t\"endTime\":\"9999-01-01 00:00:00\",\n"
            + "\t\t\t\t\t\t\t\"timezone\":\"Asia/Shanghai\",\n"
            + "\t\t\t\t\t\t\t\"delaySeconds\":0\n"
            + "\t\t\t\t\t\t},\n"
            + "\t\t\t\t\t\t\"runtimeResource\":{\n"
            + "\t\t\t\t\t\t\t\"resourceGroup\":\"S_res_group_524257424564736_1722829742200\",\n"
            + "\t\t\t\t\t\t\t\"id\":\"7011860292150347087\",\n"
            + "\t\t\t\t\t\t\t\"resourceGroupId\":\"72014319\"\n"
            + "\t\t\t\t\t\t},\n"
            + "\t\t\t\t\t\t\"name\":\"工作流跨周期小时依赖内部节点依赖外部_内部节点\",\n"
            + "\t\t\t\t\t\t\"owner\":\"206561090452322657\",\n"
            + "\t\t\t\t\t\t\"metadata\":{\n"
            + "\t\t\t\t\t\t\t\"owner\":\"206561090452322657\",\n"
            + "\t\t\t\t\t\t\t\"container\":{\n"
            + "\t\t\t\t\t\t\t\t\"type\":\"Flow\",\n"
            + "\t\t\t\t\t\t\t\t\"uuid\":\"5992975956233455901\"\n"
            + "\t\t\t\t\t\t\t},\n"
            + "\t\t\t\t\t\t\t\"ownerName\":\"lwttest04\",\n"
            + "\t\t\t\t\t\t\t\"schedulerNodeId\":700006657377,\n"
            + "\t\t\t\t\t\t\t\"tenantId\":\"524257424564736\",\n"
            + "\t\t\t\t\t\t\t\"project\":{\n"
            + "\t\t\t\t\t\t\t\t\"mode\":\"SIMPLE\",\n"
            + "\t\t\t\t\t\t\t\t\"projectOwnerId\":\"1107550004253538\",\n"
            + "\t\t\t\t\t\t\t\t\"tenantId\":\"524257424564736\",\n"
            + "\t\t\t\t\t\t\t\t\"simple\":true,\n"
            + "\t\t\t\t\t\t\t\t\"projectIdentifier\":\"lwt_test_newIde\",\n"
            + "\t\t\t\t\t\t\t\t\"projectName\":\"李文涛测试新版ide\",\n"
            + "\t\t\t\t\t\t\t\t\"projectId\":\"528891\"\n"
            + "\t\t\t\t\t\t\t},\n"
            + "\t\t\t\t\t\t\t\"projectId\":\"528891\"\n"
            + "\t\t\t\t\t\t},\n"
            + "\t\t\t\t\t\t\"inputs\":{\n"
            + "\n"
            + "\t\t\t\t\t\t},\n"
            + "\t\t\t\t\t\t\"outputs\":{\n"
            + "\t\t\t\t\t\t\t\"nodeOutputs\":[\n"
            + "\t\t\t\t\t\t\t\t{\n"
            + "\t\t\t\t\t\t\t\t\t\"data\":\"6203019234746940306\",\n"
            + "\t\t\t\t\t\t\t\t\t\"artifactType\":\"NodeOutput\",\n"
            + "\t\t\t\t\t\t\t\t\t\"refTableName\":\"工作流跨周期小时依赖内部节点依赖外部_内部节点\",\n"
            + "\t\t\t\t\t\t\t\t\t\"isDefault\":true\n"
            + "\t\t\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t\t\t]\n"
            + "\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t}\n"
            + "\t\t\t\t],\n"
            + "\t\t\t\t\"dependencies\":[\n"
            + "\t\t\t\t\t{\n"
            + "\t\t\t\t\t\t\"nodeId\":\"6203019234746940306\",\n"
            + "\t\t\t\t\t\t\"depends\":[\n"
            + "\t\t\t\t\t\t\t{\n"
            + "\t\t\t\t\t\t\t\t\"type\":\"CrossCycleDependsOnOtherNode\",\n"
            + "\t\t\t\t\t\t\t\t\"output\":\"7922382126549470808\",\n"
            + "\t\t\t\t\t\t\t\t\"refTableName\":\"工作流跨周期依赖上游\"\n"
            + "\t\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t\t]\n"
            + "\t\t\t\t\t}\n"
            + "\t\t\t\t]\n"
            + "\t\t\t}\n"
            + "\t\t]\n"
            + "\t},\n"
            + "\t\"metadata\":{\n"
            + "\t\t\"uuid\":\"6203019234746940306\",\n"
            + "\t\t\"gmtModified\":1724064225000\n"
            + "\t}\n"
            + "}";
        Specification<DataWorksWorkflowSpec> spec = SpecUtil.parseToDomain(specStr);
        SpecWorkflow workflow = spec.getSpec().getWorkflows().get(0);
        DataWorksNodeAdapter dataWorksNodeAdapter = new DataWorksNodeAdapter(spec, workflow.getNodes().get(0));
        DwNodeDependentTypeInfo info = dataWorksNodeAdapter.getDependentType(outputs -> Collections.singletonList(123456789L));
        Assert.assertNotNull(info);
        Assert.assertEquals(info.getDependentType(), DwNodeDependentTypeInfo.USER_DEFINE);
        Assert.assertNotNull(info.getDependentNodeOutputList());
        Assert.assertTrue(info.getDependentNodeOutputList().contains("7922382126549470808"));
        Assert.assertTrue(info.getDependentNodeIdList().contains(123456789L));

        Assert.assertEquals(0, (int)dataWorksNodeAdapter.getNodeType());
    }

    @Test
    public void test() {
        String spec = "{\n"
            + "        \"version\": \"1.1.0\",\n"
            + "        \"kind\": \"CycleWorkflow\",\n"
            + "        \"spec\": {\n"
            + "            \"nodes\": [\n"
            + "                {\n"
            + "                    \"recurrence\": \"Normal\",\n"
            + "                    \"id\": \"8643439\",\n"
            + "                    \"timeout\": 0,\n"
            + "                    \"instanceMode\": \"T+1\",\n"
            + "                    \"rerunMode\": \"Allowed\",\n"
            + "                    \"rerunTimes\": 0,\n"
            + "                    \"rerunInterval\": 0,\n"
            + "                    \"script\": {\n"
            + "                        \"path\": \"业务流程/DataStudio弹内/数据集成/alisa_task_history\",\n"
            + "                        \"runtime\": {\n"
            + "                            \"command\": \"DI\"\n"
            + "                        },\n"
            + "                        \"parameters\": [\n"
            + "                            {\n"
            + "                                \"name\": \"-p\\\"-Dbizdate\",\n"
            + "                                \"artifactType\": \"Variable\",\n"
            + "                                \"scope\": \"NodeParameter\",\n"
            + "                                \"type\": \"System\",\n"
            + "                                \"value\": \"$bizdate\"\n"
            + "                            },\n"
            + "                            {\n"
            + "                                \"name\": \"-Denv_path\",\n"
            + "                                \"artifactType\": \"Variable\",\n"
            + "                                \"scope\": \"NodeParameter\",\n"
            + "                                \"type\": \"System\",\n"
            + "                                \"value\": \"$env_path\"\n"
            + "                            },\n"
            + "                            {\n"
            + "                                \"name\": \"-Dhour\",\n"
            + "                                \"artifactType\": \"Variable\",\n"
            + "                                \"scope\": \"NodeParameter\",\n"
            + "                                \"type\": \"System\",\n"
            + "                                \"value\": \"$hour\"\n"
            + "                            },\n"
            + "                            {\n"
            + "                                \"name\": \"-Dtoday\",\n"
            + "                                \"artifactType\": \"Variable\",\n"
            + "                                \"scope\": \"NodeParameter\",\n"
            + "                                \"type\": \"System\",\n"
            + "                                \"value\": \"${yyyymmdd+1}\\\"\"\n"
            + "                            }\n"
            + "                        ]\n"
            + "                    },\n"
            + "                    \"trigger\": {\n"
            + "                        \"type\": \"Scheduler\",\n"
            + "                        \"cron\": \"00 03 06 * * ?\",\n"
            + "                        \"startTime\": \"1970-01-01 00:00:00\",\n"
            + "                        \"endTime\": \"9999-01-01 00:00:00\",\n"
            + "                        \"timezone\": \"Asia/Shanghai\"\n"
            + "                    },\n"
            + "                    \"runtimeResource\": {\n"
            + "                        \"resourceGroup\": \"group_20051853\",\n"
            + "                        \"resourceGroupId\": \"6\"\n"
            + "                    },\n"
            + "                    \"name\": \"alisa_task_history\",\n"
            + "                    \"owner\": \"075180\",\n"
            + "                    \"inputs\": {\n"
            + "                        \"nodeOutputs\": [\n"
            + "                            {\n"
            + "                                \"data\": \"dataworks_analyze_root\",\n"
            + "                                \"artifactType\": \"NodeOutput\"\n"
            + "                            }\n"
            + "                        ]\n"
            + "                    },\n"
            + "                    \"outputs\": {\n"
            + "                        \"nodeOutputs\": [\n"
            + "                            {\n"
            + "                                \"data\": \"dataworks_analyze.8643439_out\",\n"
            + "                                \"artifactType\": \"NodeOutput\"\n"
            + "                            }\n"
            + "                        ]\n"
            + "                    }\n"
            + "                }\n"
            + "            ],\n"
            + "            \"flow\": [\n"
            + "                {\n"
            + "                    \"nodeId\": \"8643439\",\n"
            + "                    \"depends\": [\n"
            + "                        {\n"
            + "                            \"type\": \"Normal\",\n"
            + "                            \"output\": \"dataworks_analyze_root\"\n"
            + "                        }\n"
            + "                    ]\n"
            + "                }\n"
            + "            ]\n"
            + "        }\n"
            + "    }";
        Specification<DataWorksWorkflowSpec> s = SpecUtil.parseToDomain(spec);
        DataWorksNodeAdapter adapter = new DataWorksNodeAdapter(s, s.getSpec().getNodes().get(0));
        System.out.println(adapter.getParaValue());
    }

    @Test
    public void testNoKvVariableExpression() {
        String spec = "{\n"
            + "        \"version\": \"1.1.0\",\n"
            + "        \"kind\": \"CycleWorkflow\",\n"
            + "        \"spec\": {\n"
            + "            \"nodes\": [\n"
            + "                {\n"
            + "                    \"recurrence\": \"Normal\",\n"
            + "                    \"id\": \"8643439\",\n"
            + "                    \"timeout\": 0,\n"
            + "                    \"instanceMode\": \"T+1\",\n"
            + "                    \"rerunMode\": \"Allowed\",\n"
            + "                    \"rerunTimes\": 0,\n"
            + "                    \"rerunInterval\": 0,\n"
            + "                    \"script\": {\n"
            + "                        \"path\": \"业务流程/DataStudio弹内/数据集成/alisa_task_history\",\n"
            + "                        \"runtime\": {\n"
            + "                            \"command\": \"DI\"\n"
            + "                        },\n"
            + "                        \"parameters\": [\n"
            + "                            {\n"
            + "                                \"name\": \"-\",\n"
            + "                                \"artifactType\": \"Variable\",\n"
            + "                                \"scope\": \"NodeParameter\",\n"
            + "                                \"type\": \"NoKvVariableExpression\",\n"
            + "                                \"value\": \" -p\\\"-Dbizdate=$bizdate -Denv_path=$env_path -Dhour=$hour -Dendtime=$[yyyymmdd hh24] "
            + "-Dbegintime=$[yyyymmdd hh24 - 1/24] -Dgmtdate=$gmtdate\\\"\"\n"
            + "                            }\n"
            + "                        ]\n"
            + "                    },\n"
            + "                    \"trigger\": {\n"
            + "                        \"type\": \"Scheduler\",\n"
            + "                        \"cron\": \"00 03 06 * * ?\",\n"
            + "                        \"startTime\": \"1970-01-01 00:00:00\",\n"
            + "                        \"endTime\": \"9999-01-01 00:00:00\",\n"
            + "                        \"timezone\": \"Asia/Shanghai\"\n"
            + "                    },\n"
            + "                    \"runtimeResource\": {\n"
            + "                        \"resourceGroup\": \"group_20051853\",\n"
            + "                        \"resourceGroupId\": \"6\"\n"
            + "                    },\n"
            + "                    \"name\": \"alisa_task_history\",\n"
            + "                    \"owner\": \"075180\",\n"
            + "                    \"inputs\": {\n"
            + "                        \"nodeOutputs\": [\n"
            + "                            {\n"
            + "                                \"data\": \"dataworks_analyze_root\",\n"
            + "                                \"artifactType\": \"NodeOutput\"\n"
            + "                            }\n"
            + "                        ]\n"
            + "                    },\n"
            + "                    \"outputs\": {\n"
            + "                        \"nodeOutputs\": [\n"
            + "                            {\n"
            + "                                \"data\": \"dataworks_analyze.8643439_out\",\n"
            + "                                \"artifactType\": \"NodeOutput\"\n"
            + "                            }\n"
            + "                        ]\n"
            + "                    }\n"
            + "                }\n"
            + "            ],\n"
            + "            \"flow\": [\n"
            + "                {\n"
            + "                    \"nodeId\": \"8643439\",\n"
            + "                    \"depends\": [\n"
            + "                        {\n"
            + "                            \"type\": \"Normal\",\n"
            + "                            \"output\": \"dataworks_analyze_root\"\n"
            + "                        }\n"
            + "                    ]\n"
            + "                }\n"
            + "            ]\n"
            + "        }\n"
            + "    }";
        Specification<DataWorksWorkflowSpec> s = SpecUtil.parseToDomain(spec);
        DataWorksNodeAdapter adapter = new DataWorksNodeAdapter(s, s.getSpec().getNodes().get(0));
        System.out.println(adapter.getParaValue());
        Assert.assertEquals(
            " -p\"-Dbizdate=$bizdate -Denv_path=$env_path -Dhour=$hour -Dendtime=$[yyyymmdd hh24] -Dbegintime=$[yyyymmdd hh24 - 1/24] "
                + "-Dgmtdate=$gmtdate\"",
            adapter.getParaValue());
    }

    @Test
    public void testParamNode() {
        String spec = "{\n"
            + "\t\"version\":\"1.1.0\",\n"
            + "\t\"kind\":\"CycleWorkflow\",\n"
            + "\t\"spec\":{\n"
            + "\t\t\"nodes\":[\n"
            + "\t\t\t{\n"
            + "\t\t\t\t\"recurrence\":\"Normal\",\n"
            + "\t\t\t\t\"id\":\"5518704450589103077\",\n"
            + "\t\t\t\t\"timeout\":0,\n"
            + "\t\t\t\t\"instanceMode\":\"T+1\",\n"
            + "\t\t\t\t\"rerunMode\":\"Allowed\",\n"
            + "\t\t\t\t\"rerunTimes\":3,\n"
            + "\t\t\t\t\"rerunInterval\":180000,\n"
            + "\t\t\t\t\"script\":{\n"
            + "\t\t\t\t\t\"path\":\"聿剑/General/参数节点/param13\",\n"
            + "\t\t\t\t\t\"runtime\":{\n"
            + "\t\t\t\t\t\t\"command\":\"PARAM_HUB\"\n"
            + "\t\t\t\t\t},\n"
            + "\t\t\t\t\t\"id\":\"6212993173004701817\",\n"
            + "\t\t\t\t\t\"parameters\":[\n"
            + "\t\t\t\t\t\t{\n"
            + "\t\t\t\t\t\t\t\"name\":\"p1\",\n"
            + "\t\t\t\t\t\t\t\"artifactType\":\"Variable\",\n"
            + "\t\t\t\t\t\t\t\"scope\":\"NodeContext\",\n"
            + "\t\t\t\t\t\t\t\"type\":\"Constant\",\n"
            + "\t\t\t\t\t\t\t\"value\":\"ppppp111\",\n"
            + "\t\t\t\t\t\t\t\"id\":\"6368748218498953492\"\n"
            + "\t\t\t\t\t\t},\n"
            + "\t\t\t\t\t\t{\n"
            + "\t\t\t\t\t\t\t\"name\":\"var1\",\n"
            + "\t\t\t\t\t\t\t\"artifactType\":\"Variable\",\n"
            + "\t\t\t\t\t\t\t\"scope\":\"NodeContext\",\n"
            + "\t\t\t\t\t\t\t\"type\":\"System\",\n"
            + "\t\t\t\t\t\t\t\"value\":\"${yyyyMMdd}\",\n"
            + "\t\t\t\t\t\t\t\"id\":\"5696339307604082769\"\n"
            + "\t\t\t\t\t\t},\n"
            + "\t\t\t\t\t\t{\n"
            + "\t\t\t\t\t\t\t\"name\":\"passVar1\",\n"
            + "\t\t\t\t\t\t\t\"artifactType\":\"Variable\",\n"
            + "\t\t\t\t\t\t\t\"scope\":\"NodeContext\",\n"
            + "\t\t\t\t\t\t\t\"type\":\"PassThrough\",\n"
            + "\t\t\t\t\t\t\t\"value\":\"5954133462609987429:outputs\",\n"
            + "\t\t\t\t\t\t\t\"id\":\"6484800623738283008\",\n"
            + "\t\t\t\t\t\t\t\"node\":{\n"
            + "\t\t\t\t\t\t\t\t\"output\":\"5954133462609987429\"\n"
            + "\t\t\t\t\t\t\t},\n"
            + "\t\t\t\t\t\t\t\"referenceVariable\":{\n"
            + "\t\t\t\t\t\t\t\t\"name\":\"outputs\",\n"
            + "\t\t\t\t\t\t\t\t\"artifactType\":\"Variable\",\n"
            + "\t\t\t\t\t\t\t\t\"scope\":\"NodeContext\",\n"
            + "\t\t\t\t\t\t\t\t\"type\":\"NodeOutput\",\n"
            + "\t\t\t\t\t\t\t\t\"value\":\"5954133462609987429:outputs\",\n"
            + "\t\t\t\t\t\t\t\t\"node\":{\n"
            + "\t\t\t\t\t\t\t\t\t\"output\":\"5954133462609987429\"\n"
            + "\t\t\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t]\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t\"trigger\":{\n"
            + "\t\t\t\t\t\"type\":\"Scheduler\",\n"
            + "\t\t\t\t\t\"id\":\"5252195676878025242\",\n"
            + "\t\t\t\t\t\"cron\":\"00 00 00 * * ?\",\n"
            + "\t\t\t\t\t\"startTime\":\"1970-01-01 00:00:00\",\n"
            + "\t\t\t\t\t\"endTime\":\"9999-01-01 00:00:00\",\n"
            + "\t\t\t\t\t\"timezone\":\"Asia/Shanghai\"\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t\"runtimeResource\":{\n"
            + "\t\t\t\t\t\"resourceGroup\":\"group_2\",\n"
            + "\t\t\t\t\t\"id\":\"5623679673296125496\",\n"
            + "\t\t\t\t\t\"resourceGroupId\":\"2\"\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t\"name\":\"param13\",\n"
            + "\t\t\t\t\"owner\":\"064152\",\n"
            + "\t\t\t\t\"inputs\":{\n"
            + "\t\t\t\t\t\"nodeOutputs\":[\n"
            + "\t\t\t\t\t\t{\n"
            + "\t\t\t\t\t\t\t\"data\":\"5954133462609987429\",\n"
            + "\t\t\t\t\t\t\t\"artifactType\":\"NodeOutput\",\n"
            + "\t\t\t\t\t\t\t\"refTableName\":\"赋值1\"\n"
            + "\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t],\n"
            + "\t\t\t\t\t\"variables\":[\n"
            + "\t\t\t\t\t\t{\n"
            + "\t\t\t\t\t\t\t\"name\":\"outputs\",\n"
            + "\t\t\t\t\t\t\t\"artifactType\":\"Variable\",\n"
            + "\t\t\t\t\t\t\t\"scope\":\"NodeContext\",\n"
            + "\t\t\t\t\t\t\t\"type\":\"NodeOutput\",\n"
            + "\t\t\t\t\t\t\t\"value\":\"5954133462609987429:outputs\",\n"
            + "\t\t\t\t\t\t\t\"id\":\"6016027971803201307\",\n"
            + "\t\t\t\t\t\t\t\"node\":{\n"
            + "\t\t\t\t\t\t\t\t\"output\":\"5954133462609987429\"\n"
            + "\t\t\t\t\t\t\t},\n"
            + "\t\t\t\t\t\t\t\"referenceVariable\":{\n"
            + "\t\t\t\t\t\t\t\t\"name\":\"outputs\",\n"
            + "\t\t\t\t\t\t\t\t\"artifactType\":\"Variable\",\n"
            + "\t\t\t\t\t\t\t\t\"scope\":\"NodeContext\",\n"
            + "\t\t\t\t\t\t\t\t\"type\":\"NodeOutput\",\n"
            + "\t\t\t\t\t\t\t\t\"value\":\"5954133462609987429:outputs\",\n"
            + "\t\t\t\t\t\t\t\t\"node\":{\n"
            + "\t\t\t\t\t\t\t\t\t\"output\":\"5954133462609987429\"\n"
            + "\t\t\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t]\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t\"outputs\":{\n"
            + "\t\t\t\t\t\"nodeOutputs\":[\n"
            + "\t\t\t\t\t\t{\n"
            + "\t\t\t\t\t\t\t\"data\":\"5518704450589103077\",\n"
            + "\t\t\t\t\t\t\t\"artifactType\":\"NodeOutput\",\n"
            + "\t\t\t\t\t\t\t\"refTableName\":\"param13\"\n"
            + "\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t],\n"
            + "\t\t\t\t\t\"variables\":[\n"
            + "\t\t\t\t\t\t{\n"
            + "\t\t\t\t\t\t\t\"name\":\"p1\",\n"
            + "\t\t\t\t\t\t\t\"artifactType\":\"Variable\",\n"
            + "\t\t\t\t\t\t\t\"scope\":\"NodeContext\",\n"
            + "\t\t\t\t\t\t\t\"type\":\"Constant\",\n"
            + "\t\t\t\t\t\t\t\"value\":\"ppppp111\",\n"
            + "\t\t\t\t\t\t\t\"id\":\"7879674560462811039\",\n"
            + "\t\t\t\t\t\t\t\"node\":{\n"
            + "\t\t\t\t\t\t\t\t\"output\":\"5518704450589103077\"\n"
            + "\t\t\t\t\t\t\t},\n"
            + "\t\t\t\t\t\t\t\"referenceVariable\":{\n"
            + "\t\t\t\t\t\t\t\t\"name\":\"p1\",\n"
            + "\t\t\t\t\t\t\t\t\"artifactType\":\"Variable\",\n"
            + "\t\t\t\t\t\t\t\t\"scope\":\"NodeContext\",\n"
            + "\t\t\t\t\t\t\t\t\"type\":\"Constant\",\n"
            + "\t\t\t\t\t\t\t\t\"value\":\"ppppp111\",\n"
            + "\t\t\t\t\t\t\t\t\"node\":{\n"
            + "\t\t\t\t\t\t\t\t\t\"output\":\"5518704450589103077\"\n"
            + "\t\t\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t\t},\n"
            + "\t\t\t\t\t\t{\n"
            + "\t\t\t\t\t\t\t\"name\":\"passVar1\",\n"
            + "\t\t\t\t\t\t\t\"artifactType\":\"Variable\",\n"
            + "\t\t\t\t\t\t\t\"scope\":\"NodeContext\",\n"
            + "\t\t\t\t\t\t\t\"type\":\"PassThrough\",\n"
            + "\t\t\t\t\t\t\t\"value\":\"5518704450589103077:passVar1\",\n"
            + "\t\t\t\t\t\t\t\"id\":\"7403543240975774833\",\n"
            + "\t\t\t\t\t\t\t\"node\":{\n"
            + "\t\t\t\t\t\t\t\t\"output\":\"5518704450589103077\"\n"
            + "\t\t\t\t\t\t\t},\n"
            + "\t\t\t\t\t\t\t\"referenceVariable\":{\n"
            + "\t\t\t\t\t\t\t\t\"name\":\"passVar1\",\n"
            + "\t\t\t\t\t\t\t\t\"artifactType\":\"Variable\",\n"
            + "\t\t\t\t\t\t\t\t\"scope\":\"NodeContext\",\n"
            + "\t\t\t\t\t\t\t\t\"type\":\"PassThrough\",\n"
            + "\t\t\t\t\t\t\t\t\"value\":\"5954133462609987429:outputs\",\n"
            + "\t\t\t\t\t\t\t\t\"node\":{\n"
            + "\t\t\t\t\t\t\t\t\t\"output\":\"5518704450589103077\"\n"
            + "\t\t\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t\t},\n"
            + "\t\t\t\t\t\t{\n"
            + "\t\t\t\t\t\t\t\"name\":\"var1\",\n"
            + "\t\t\t\t\t\t\t\"artifactType\":\"Variable\",\n"
            + "\t\t\t\t\t\t\t\"scope\":\"NodeContext\",\n"
            + "\t\t\t\t\t\t\t\"type\":\"System\",\n"
            + "\t\t\t\t\t\t\t\"value\":\"${yyyyMMdd}\",\n"
            + "\t\t\t\t\t\t\t\"id\":\"6259613835245088211\",\n"
            + "\t\t\t\t\t\t\t\"node\":{\n"
            + "\t\t\t\t\t\t\t\t\"output\":\"5518704450589103077\"\n"
            + "\t\t\t\t\t\t\t},\n"
            + "\t\t\t\t\t\t\t\"referenceVariable\":{\n"
            + "\t\t\t\t\t\t\t\t\"name\":\"var1\",\n"
            + "\t\t\t\t\t\t\t\t\"artifactType\":\"Variable\",\n"
            + "\t\t\t\t\t\t\t\t\"scope\":\"NodeContext\",\n"
            + "\t\t\t\t\t\t\t\t\"type\":\"System\",\n"
            + "\t\t\t\t\t\t\t\t\"value\":\"${yyyyMMdd}\",\n"
            + "\t\t\t\t\t\t\t\t\"node\":{\n"
            + "\t\t\t\t\t\t\t\t\t\"output\":\"5518704450589103077\"\n"
            + "\t\t\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t]\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t\"param-hub\":{\n"
            + "\t\t\t\t\t\"variables\":[\n"
            + "\t\t\t\t\t\t{\n"
            + "\t\t\t\t\t\t\t\"name\":\"p1\",\n"
            + "\t\t\t\t\t\t\t\"artifactType\":\"Variable\",\n"
            + "\t\t\t\t\t\t\t\"scope\":\"NodeContext\",\n"
            + "\t\t\t\t\t\t\t\"type\":\"Constant\",\n"
            + "\t\t\t\t\t\t\t\"value\":\"ppppp111\",\n"
            + "\t\t\t\t\t\t\t\"description\":\"111\"\n"
            + "\t\t\t\t\t\t},\n"
            + "\t\t\t\t\t\t{\n"
            + "\t\t\t\t\t\t\t\"name\":\"var1\",\n"
            + "\t\t\t\t\t\t\t\"artifactType\":\"Variable\",\n"
            + "\t\t\t\t\t\t\t\"scope\":\"NodeContext\",\n"
            + "\t\t\t\t\t\t\t\"type\":\"System\",\n"
            + "\t\t\t\t\t\t\t\"value\":\"${yyyyMMdd}\",\n"
            + "\t\t\t\t\t\t\t\"description\":\"var1\"\n"
            + "\t\t\t\t\t\t},\n"
            + "\t\t\t\t\t\t{\n"
            + "\t\t\t\t\t\t\t\"name\":\"passVar1\",\n"
            + "\t\t\t\t\t\t\t\"artifactType\":\"Variable\",\n"
            + "\t\t\t\t\t\t\t\"scope\":\"NodeContext\",\n"
            + "\t\t\t\t\t\t\t\"type\":\"PassThrough\",\n"
            + "\t\t\t\t\t\t\t\"description\":\"passVar1\",\n"
            + "\t\t\t\t\t\t\t\"referenceVariable\":{\n"
            + "\t\t\t\t\t\t\t\t\"name\":\"outputs\",\n"
            + "\t\t\t\t\t\t\t\t\"artifactType\":\"Variable\",\n"
            + "\t\t\t\t\t\t\t\t\"scope\":\"NodeContext\",\n"
            + "\t\t\t\t\t\t\t\t\"type\":\"NodeOutput\",\n"
            + "\t\t\t\t\t\t\t\t\"node\":{\n"
            + "\t\t\t\t\t\t\t\t\t\"output\":\"5954133462609987429\"\n"
            + "\t\t\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t\t}\n"
            + "\t\t\t\t\t]\n"
            + "\t\t\t\t}\n"
            + "\t\t\t}\n"
            + "\t\t],\n"
            + "\t\t\"flow\":[\n"
            + "\t\t\t{\n"
            + "\t\t\t\t\"nodeId\":\"5518704450589103077\",\n"
            + "\t\t\t\t\"depends\":[\n"
            + "\t\t\t\t\t{\n"
            + "\t\t\t\t\t\t\"type\":\"Normal\",\n"
            + "\t\t\t\t\t\t\"output\":\"5954133462609987429\"\n"
            + "\t\t\t\t\t}\n"
            + "\t\t\t\t]\n"
            + "\t\t\t}\n"
            + "\t\t]\n"
            + "\t}\n"
            + "}";

        Specification<DataWorksWorkflowSpec> sp = SpecUtil.parseToDomain(spec);
        DataWorksNodeAdapter adapter = new DataWorksNodeAdapter(sp, sp.getSpec().getNodes().get(0));
        log.info("input context: {}", adapter.getInputContexts());
        log.info("output context: {}", adapter.getOutputContexts());

        Assert.assertNotNull(adapter.getInputContexts());
        Assert.assertNotNull(adapter.getOutputContexts());
        Assert.assertTrue(adapter.getOutputContexts().stream().filter(oc -> oc.getKey().equals("passVar1"))
            .anyMatch(oc -> oc.getValueExpr().equals("5954133462609987429:outputs")));
        Assert.assertTrue(adapter.getOutputContexts().stream().filter(oc -> oc.getKey().equals("var1"))
            .anyMatch(oc -> oc.getValueExpr().equals("${yyyyMMdd}")));
        Assert.assertTrue(adapter.getOutputContexts().stream().filter(oc -> oc.getKey().equals("p1"))
            .anyMatch(oc -> oc.getValueExpr().equals("ppppp111")));
    }

    @Test
    public void testManual() throws IOException {
        String spec = IOUtils.toString(
            Objects.requireNonNull(DataWorksNodeAdapterTest.class.getClassLoader().getResource("nodemodel/manual.json")),
            StandardCharsets.UTF_8);

        System.out.println(spec);
        Specification<DataWorksWorkflowSpec> specObj = SpecUtil.parseToDomain(spec);
        Assert.assertNotNull(specObj);

        DataWorksWorkflowSpec specification = specObj.getSpec();
        Assert.assertNotNull(specification);
        Assert.assertEquals(1, CollectionUtils.size(specification.getNodes()));

        SpecNode shellNode = specification.getNodes().get(0);
        Assert.assertNotNull(shellNode);

        shellNode.setIgnoreBranchConditionSkip(true);
        shellNode.setTimeout(0);

        DataWorksNodeAdapter adapter = new DataWorksNodeAdapter(specObj, shellNode);
        log.info("para value: {}", adapter.getParaValue());
        Assert.assertNotNull(adapter.getParaValue());
        Assert.assertEquals("2=222222 1=111111", adapter.getParaValue());
        Assert.assertEquals(1, (int)adapter.getNodeType());
    }

    @Test
    public void testGetPrgType() {
        Specification<DataWorksWorkflowSpec> spec = new Specification<>();
        SpecNode node = new SpecNode();
        SpecScript script = new SpecScript();
        SpecScriptRuntime runtime1 = new SpecScriptRuntime();
        runtime1.setCommand("ODPS_SQL");
        script.setRuntime(runtime1);
        node.setScript(script);

        DataWorksNodeAdapter adapter = new DataWorksNodeAdapter(spec, node);
        Assert.assertEquals(10, (int)adapter.getPrgType(s -> CodeProgramType.getNodeTypeByName(s).getCode()));

        SpecScriptRuntime runtime2 = new SpecScriptRuntime();
        runtime2.setCommand("MySQL");
        runtime2.setCommandTypeId(1000039);
        script.setRuntime(runtime2);
        adapter = new DataWorksNodeAdapter(spec, node);
        Assert.assertEquals(1000039, (int)adapter.getPrgType(s -> CodeProgramType.getNodeTypeByName(s).getCode()));
    }

    @Test
    public void testComponentSqlCode() {
        String content = "select '@@{p1}', '@@{p2}', '@@{p3}';";

        Specification<DataWorksWorkflowSpec> specification = new Specification<>();
        DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
        SpecNode specNode = new SpecNode();
        SpecScript script = new SpecScript();
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setCommand(CodeProgramType.COMPONENT_SQL.getName());
        script.setContent(content);
        script.setRuntime(runtime);
        specNode.setScript(script);
        SpecComponent component = new SpecComponent();
        SpecComponentParameter in1 = new SpecComponentParameter();
        in1.setName("p1");
        in1.setValue("var1");
        component.setInputs(Collections.singletonList(in1));
        specNode.setComponent(component);
        spec.setNodes(Collections.singletonList(specNode));
        specification.setSpec(spec);

        DataWorksNodeAdapter adapter = new DataWorksNodeAdapter(specification, specNode, Context.builder()
            .deployToScheduler(true)
            .build());
        String code = adapter.getCode();
        log.info("code: {}", code);
        Assert.assertEquals("select 'var1', '@@{p2}', '@@{p3}';", code);
    }

    @Test
    public void testWorkflow() {
        Specification<DataWorksWorkflowSpec> specification = new Specification<>();
        DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
        SpecWorkflow specWorkflow = new SpecWorkflow();
        SpecNodeOutput input = new SpecNodeOutput();
        input.setData("autotest.123_out");
        specWorkflow.setInputs(Collections.singletonList(input));
        SpecNodeOutput output = new SpecNodeOutput();
        output.setData("autotest.456_out");
        specWorkflow.setOutputs(Collections.singletonList(output));

        SpecScript script = new SpecScript();
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setCommand("WORKFLOW");
        script.setRuntime(runtime);
        specWorkflow.setScript(script);
        SpecScheduleStrategy st = new SpecScheduleStrategy();
        st.setRecurrenceType(NodeRecurrenceType.NONE_AUTO);
        specWorkflow.setStrategy(st);
        spec.setWorkflows(Collections.singletonList(specWorkflow));

        DataWorksNodeAdapter adapter = new DataWorksNodeAdapter(specification, specWorkflow);
        Assert.assertEquals(4, (int)adapter.getNodeType());
        Assert.assertNotNull(adapter.getInputs());
        Assert.assertEquals(1, adapter.getInputs().size());
        Assert.assertNotNull(adapter.getOutputs());
        Assert.assertEquals(1, adapter.getOutputs().size());
        Assert.assertEquals(123, (int)adapter.getPrgType(cmd -> 123));
    }

    @Test
    public void testStreamLaunchMode() {
        Specification<DataWorksWorkflowSpec> sp = new Specification<>();
        DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
        SpecNode node = new SpecNode();
        node.setId("11");
        node.setName("sparkstreaming1");
        SpecScript script = new SpecScript();
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        Map<String, Object> config = new HashMap<>();
        config.put(DataWorksNodeAdapter.STREAM_LAUNCH_MODE, 1);
        runtime.setStreamJobConfig(config);
        script.setRuntime(runtime);
        node.setScript(script);
        spec.setNodes(Collections.singletonList(node));
        sp.setSpec(spec);

        DataWorksNodeAdapter dataWorksNodeAdapter = new DataWorksNodeAdapter(sp, node);
        Map<String, Object> extraConf = dataWorksNodeAdapter.getExtConfig();
        Assert.assertNotNull(extraConf);
        Assert.assertEquals(1, extraConf.get(DataWorksNodeAdapter.STREAM_LAUNCH_MODE));
    }

    @Test
    public void testGetAdvanceSettings() {
        Specification<DataWorksWorkflowSpec> sp = new Specification<>();
        DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
        SpecNode node = new SpecNode();
        node.setId("adb-spark-0");
        node.setName("adb-spark-0");
        SpecScript script = new SpecScript();
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        Map<String, Object> config = new HashMap<>();
        config.put("resourceGroupName", "spark_test");
        runtime.setAdbJobConfig(config);
        Map<String, Object> sparkConf = new HashMap<>();
        sparkConf.put("spark.executor.cores", 4);
        runtime.setSparkConf(sparkConf);
        runtime.setCommand(CodeProgramType.ADB_SPARK.name());
        script.setRuntime(runtime);
        JSONObject sparkSubmitCode = new JSONObject();
        sparkSubmitCode.put("command", "spark-submit --master yarn");
        script.setContent(sparkSubmitCode.toJSONString());
        node.setScript(script);
        spec.setNodes(Collections.singletonList(node));
        sp.setSpec(spec);

        DataWorksNodeAdapter dataWorksNodeAdapter = new DataWorksNodeAdapter(sp, node);
        dataWorksNodeAdapter.setContext(Context.builder().deployToScheduler(true).build());
        JSONObject advanceSettings = JSON.parseObject(dataWorksNodeAdapter.getAdvanceSettings());
        log.info("advance settings: {}, code: {}", advanceSettings, dataWorksNodeAdapter.getCode());
        Assert.assertNotNull(advanceSettings);
        Assert.assertEquals("spark_test", advanceSettings.getString("resourceGroupName"));
        Assert.assertEquals("4", advanceSettings.getString("spark.executor.cores"));
        Assert.assertEquals(4, (int)advanceSettings.getInteger("spark.executor.cores"));
        Assert.assertEquals("spark-submit --master yarn", dataWorksNodeAdapter.getCode());
    }
}
