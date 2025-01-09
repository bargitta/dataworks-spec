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

package com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.Project;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.ProcessMeta;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.TaskNode;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.UdfFunc;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.datasource.DataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.entity.ResourceInfo;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.math.NumberUtils;

public class DolphinSchedulerV1Context {

    private static DolphinSchedulerV1Context context;

    private List<Project> projects;

    private List<DataSource> dataSources;
    private List<ResourceInfo> resources;
    private List<UdfFunc> udfFuncs;

    private List<ProcessMeta> dagDatas;
    private Map<Long, Project> projectCodeMap = new HashMap<>();

    private Map<Integer, List<String>> subProcessCodeOutMap = new HashMap<>();

    public Map<Integer, List<Object>> getProcessCodeTaskRelationMap() {
        return processCodeTaskRelationMap;
    }

    private Map<Integer, List<Object>> processCodeTaskRelationMap = new HashMap<>();

    /**
     * processId：SpecNode
     */
    private Map<Integer, Object> subProcessCodeNodeMap = new HashMap<>();
    /**
     * processId: workflow
     */
    private Map<Integer, Object> subProcessCodeWorkflowMap = new HashMap<>();

    /**
     * taskDefinition.code : SpecNodeOutput.data
     */
    private Map<String, String> taskCodeNodeDataMap = new HashMap<>();
    private Map<String, String> taskCodeNodeIdMap = new HashMap<>();

    public Map<String, Object> getTaskCodeSpecNodeMap() {
        return taskCodeSpecNodeMap;
    }

    private Map<String, Object> taskCodeSpecNodeMap = new HashMap<>();

    private Map<Object, List<String>> specNodeProcessCodeMap = new HashMap<>();

    private DolphinSchedulerV1Context() {

    }

    public static void initContext(List<Project> projects, List<ProcessMeta> dagDatas, List<DataSource> dataSources,
            List<ResourceInfo> resources, List<UdfFunc> udfFuncs) {
        DolphinSchedulerV1Context context = new DolphinSchedulerV1Context();
        context.projects = projects;
        context.dagDatas = dagDatas;
        context.dataSources = dataSources;
        context.resources = resources;
        context.udfFuncs = udfFuncs;
        for (Project project : ListUtils.emptyIfNull(projects)) {
            //dolphin1 has not code
            if(NumberUtils.isDigits(project.getCode())){
                context.projectCodeMap.put(Long.parseLong(project.getCode()), project);
            }
        }

        for (ProcessMeta processMeta : dagDatas) {
            List<TaskNode> taskNodes = processMeta.getProcessDefinitionJson().getTasks();
            List<Object> taskDefinitionList = context.processCodeTaskRelationMap.get(processMeta.getProcessDefinitionId());
            if (taskDefinitionList == null) {
                taskDefinitionList = new ArrayList<>();
                context.processCodeTaskRelationMap.put(processMeta.getProcessDefinitionId(), taskDefinitionList);
            }
            taskDefinitionList.addAll(taskNodes);
        }

        DolphinSchedulerV1Context.context = context;
    }

    public static DolphinSchedulerV1Context getContext() {
        return DolphinSchedulerV1Context.context;
    }

    public Map<Long, Project> getProjectCodeMap() {
        return projectCodeMap;
    }

    public void setProjectCodeMap(Map<Long, Project> projectCodeMap) {
        this.projectCodeMap = projectCodeMap;
    }

    public List<Project> getProjects() {
        return projects;
    }

    public void setProjects(List<Project> projects) {
        this.projects = projects;
    }

    public List<DataSource> getDataSources() {
        return dataSources;
    }

    public void setDataSources(List<DataSource> dataSources) {
        this.dataSources = dataSources;
    }

    public List<ResourceInfo> getResources() {
        return resources;
    }

    public void setResources(List<ResourceInfo> resources) {
        this.resources = resources;
    }

    public List<UdfFunc> getUdfFuncs() {
        return udfFuncs;
    }

    public void setUdfFuncs(List<UdfFunc> udfFuncs) {
        this.udfFuncs = udfFuncs;
    }

    public void putSubProcessCodeOutMap(Integer code, String out) {
        List<String> outs = this.subProcessCodeOutMap.get(code);
        if (outs == null) {
            outs = new ArrayList<>();
            this.subProcessCodeOutMap.put(code, outs);
        }
        outs.add(out);
    }

    public List<String> getSubProcessCodeMap(Long code) {
        return this.subProcessCodeOutMap.get(code);
    }

    public Map<String, String> getTaskCodeNodeDataMap() {
        return taskCodeNodeDataMap;
    }

    public Map<String, String> getTaskCodeNodeIdMap() {
        return taskCodeNodeIdMap;
    }

    public Map<Integer, Object> getSubProcessCodeNodeMap() {
        return subProcessCodeNodeMap;
    }

    public Map<Integer, Object> getSubProcessCodeWorkflowMap() {
        return subProcessCodeWorkflowMap;
    }

    public Map<Object, List<String>> getSpecNodeProcessCodeMap() {
        return specNodeProcessCodeMap;
    }
}
