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

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.dw.types.CalcEngineType;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.dw.types.LabelType;
import com.aliyun.dataworks.common.spec.domain.enums.SpecFileResourceType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.DolphinSchedulerPackage;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.DolphinSchedulerVersion;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.datasource.BaseDataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.v139.datasource.DataSourceFactory;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.TaskType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DolphinSchedulerV3Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.ProcessDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.DataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.ResourceComponent;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.UdfFunc;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.DbType;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.Asset;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DwDatasource;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DwResource;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DwWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.Resource;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.connection.JdbcConnection;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.types.AssetType;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.types.tenant.EnvType;
import com.aliyun.dataworks.migrationx.transformer.core.common.Constants;
import com.aliyun.dataworks.migrationx.transformer.core.loader.ProjectAssetLoader;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.AbstractDolphinSchedulerConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.DolphinSchedulerConverterContext;
import com.aliyun.dataworks.migrationx.transformer.dataworks.transformer.DataWorksTransformerConfig;
import com.aliyun.migrationx.common.context.TransformerContext;
import com.aliyun.migrationx.common.exception.UnSupportedTypeException;
import com.aliyun.migrationx.common.utils.Config;
import com.aliyun.migrationx.common.utils.GsonUtils;

import com.google.common.base.Joiner;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * for dolphinscheduler v2.0.1 and later convert dolphinscheduler process to dataworks model
 *
 * @author 聿剑
 * @date 2022/10/12
 */
@Slf4j
public class DolphinSchedulerV3Converter extends AbstractDolphinSchedulerConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DolphinSchedulerV3Converter.class);

    public static final DolphinSchedulerVersion version = DolphinSchedulerVersion.V2;

    private List<DagData> dagDataList = new ArrayList<>();
    private List<DwWorkflow> dwWorkflowList = new ArrayList<>();
    private DolphinSchedulerPackage<com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.Project, DagData,
            DataSource, ResourceComponent, UdfFunc> dolphinSchedulerPackage;

    public DolphinSchedulerV3Converter(DolphinSchedulerPackage dolphinSchedulerPackage) {
        super(AssetType.DOLPHINSCHEDULER, DolphinSchedulerV3Converter.class.getSimpleName());
        this.dolphinSchedulerPackage = dolphinSchedulerPackage;
    }

    public DolphinSchedulerV3Converter(AssetType assetType, String name) {
        super(assetType, name);
    }

    public DolphinSchedulerV3Converter(AssetType assetType, String name,
            ProjectAssetLoader projectAssetLoader) {
        super(assetType, name, projectAssetLoader);
    }

    @Override
    public List<DwWorkflow> convert(Asset asset) throws Exception {
        this.dagDataList = dolphinSchedulerPackage.getProcessDefinitions().values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
        if (dagDataList.isEmpty()) {
            throw new RuntimeException("process list empty");
        }
        log.info("dagDataList size: {}", CollectionUtils.size(dagDataList));
        findAllSubProcessDefinition(dagDataList);
        dwWorkflowList = convertProcessMetaListToDwWorkflowList(dagDataList);
        //processSubProcessDefinitionDepends();
        setProjectRootDependForNoInputNode(project, dwWorkflowList);
        convertDataSources(project);
        //todo add all resources
        //setResources(dwWorkflowList.size() > 0 ? dwWorkflowList.get(0) : null);
        return dwWorkflowList;
    }

    private void setResources(DwWorkflow workflow) {
        if (workflow == null) {
            return;
        }

        String engineType = properties.getProperty(Constants.CONVERTER_TARGET_ENGINE_TYPE, "");
        CalcEngineType calcEngineType = CalcEngineType.valueOf(engineType);
        List<String> paths = new ArrayList<>();
        DataWorksTransformerConfig config = DataWorksTransformerConfig.getConfig();
        if (config != null) {
            paths.add(calcEngineType.getDisplayName(config.getLocale()));
            paths.add(LabelType.RESOURCE.getDisplayName(config.getLocale()));
        } else {
            paths.add(calcEngineType.getDisplayName(Locale.SIMPLIFIED_CHINESE));
            paths.add(LabelType.RESOURCE.getDisplayName(Locale.SIMPLIFIED_CHINESE));
        }

        String folder = Joiner.on(File.separator).join(paths);
        List<Resource> resources = new ArrayList<>();

        List<ResourceComponent> resourceComponents = dolphinSchedulerPackage.getResources();
        File dir = TransformerContext.getContext().getSourceDir();

        for (ResourceComponent component : resourceComponents) {
            File file = new File(dir.getAbsolutePath()
                    + File.separator + "resource" + File.separator + "resources"
                    + File.separator + component.getFileName());
            if (!file.exists()) {
                continue;
            }
            DwResource resource = new DwResource();
            resource.setName(component.getFileName());
            resource.setFolder(folder);
            if (component.getFileName().endsWith(".jar")) {
                if (CalcEngineType.EMR.equals(calcEngineType)) {
                    resource.setType(CodeProgramType.EMR_JAR.name());
                } else if (CalcEngineType.ODPS.equals(calcEngineType)) {
                    resource.setType(CodeProgramType.ODPS_JAR.name());
                } else {
                    continue;
                }
                resource.setExtend(SpecFileResourceType.JAR.name());
            } else if (component.getFileName().endsWith(".py")) {
                if (CalcEngineType.ODPS.equals(calcEngineType)) {
                    resource.setType(CodeProgramType.ODPS_PYTHON.name());
                } else {
                    continue;
                }
                resource.setExtend(SpecFileResourceType.PYTHON.name());
            } else if (component.getFileName().endsWith(".txt")) {
                if (CalcEngineType.EMR.equals(calcEngineType)) {
                    resource.setType(CodeProgramType.EMR_FILE.name());
                } else {
                    continue;
                }
                resource.setExtend(SpecFileResourceType.FILE.name());
            } else if (component.getFileName().endsWith(".sh")) {
                if (CalcEngineType.EMR.equals(calcEngineType)) {
                    resource.setType(CodeProgramType.EMR_FILE.name());
                } else if (CalcEngineType.ODPS.equals(calcEngineType)) {
                    resource.setType(CodeProgramType.ODPS_FILE.name());
                } else {
                    continue;
                }
                resource.setExtend(SpecFileResourceType.FILE.name());
            } else {
                continue;
            }
            resource.setLocalPath(file.getAbsolutePath());
            resources.add(resource);
        }

        workflow.getResources().addAll(resources);
    }

    private List<DwWorkflow> convertProcessMetaListToDwWorkflowList(List<DagData> dataList) {
        return ListUtils.emptyIfNull(dataList).stream()
                .map(this::convertProcessMetaToDwWorkflow)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    private List<DwWorkflow> convertProcessMetaToDwWorkflow(DagData processMeta) {
        log.info("convertProcessMetaToDwWorkflow: {}", processMeta.getProcessDefinition().getName());
        DolphinSchedulerConverterContext<com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.Project, DagData,
                DataSource, ResourceComponent, UdfFunc>
                converterContext = new DolphinSchedulerConverterContext<>();
        converterContext.setProject(project);
        converterContext.setProperties(properties);
        converterContext.setDolphinSchedulerPackage(dolphinSchedulerPackage);
        V3ProcessDefinitionConverter definitionConverter = new V3ProcessDefinitionConverter(converterContext, processMeta);
        try {
            return definitionConverter.convert();
        } catch (UnSupportedTypeException e) {
            log.error("", e);
            if (Config.get().isSkipUnSupportType()) {
                return Collections.emptyList();
            } else {
                throw e;
            }
        } catch (Exception e) {
            throw e;
        }
    }

    private void convertDataSources(com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.Project project) {
        project.setDatasources(ListUtils.emptyIfNull(dolphinSchedulerPackage.getDatasources()).stream()
                .filter(Objects::nonNull)
                .map(ds -> {
                    DwDatasource dwDatasource = new DwDatasource();
                    dwDatasource.setName(ds.getName());
                    dwDatasource.setEnvType(EnvType.PRD.name());

                    try {
                        dwDatasource.setType(StringUtils.lowerCase(ds.getType().name()));
                        DbType dbType = DbType.valueOf(ds.getType().name());
                        BaseDataSource baseDataSource = DataSourceFactory.getDatasource(ds.getType().name(), ds.getConnectionParams());
                        Optional.ofNullable(baseDataSource).ifPresent(datasource -> {
                            switch (dbType) {
                                case MYSQL:
                                case POSTGRESQL:
                                case ORACLE:
                                case H2:
                                case DB2:
                                case CLICKHOUSE:
                                case SQLSERVER:
                                    setJdbcConnection(datasource, dwDatasource);
                                    break;
                                case SPARK:
                                case HIVE:
                            }
                        });

                        dwDatasource.setDescription(ds.getNote());
                    } catch (Exception e) {
                        log.error("can not handle type {}", ds);
                        return null;
                    }
                    return dwDatasource;
                }).filter(Objects::nonNull)
                .collect(Collectors.toList()));
    }

    /**
     * 1. create virtual begin node (SubProcessParameterConverter)
     * 2. find root tasks of process definition of subprocess
     * 3. set task pre dependent to virtual start node
     */
    private void findAllSubProcessDefinition(List<DagData> dagDataList) {
        for (DagData dagData : ListUtils.emptyIfNull(dagDataList)) {
            ProcessDefinition processDefinition = dagData.getProcessDefinition();
            dagData.getTaskDefinitionList().stream()
                    .filter(task -> TaskType.SUB_PROCESS.name().equalsIgnoreCase(task.getTaskType()))
                    .forEach(task -> {
                                JsonObject jsonObject = GsonUtils.fromJsonString(task.getTaskParams(), JsonObject.class);
                                if (jsonObject.has("processDefinitionCode")) {
                                    Long processDefCode = jsonObject.get("processDefinitionCode").getAsLong();
                                    //task dependent process code
                                    String out = getDefaultNodeOutput(processDefinition, task.getName());
                                    DolphinSchedulerV3Context.getContext().putSubProcessCodeOutMap(processDefCode, out + ".virtual.start");
                                }
                            }
                    );
        }
    }

    protected String getDefaultNodeOutput(ProcessDefinition processMeta, String taskName) {
        return Joiner.on(".").join(
                //dataworks project
                project.getName(),
                processMeta.getProjectName(),
                processMeta.getName(),
                taskName);
    }

    private void setJdbcConnection(BaseDataSource datasource, DwDatasource dwDatasource) {
        JdbcConnection conn = new JdbcConnection();
        conn.setUsername(datasource.getUser());
        conn.setPassword(datasource.getPassword());
        conn.setDatabase(datasource.getDatabase());
        conn.setJdbcUrl(datasource.getAddress());
        conn.setTag("public");
        dwDatasource.setConnection(GsonUtils.defaultGson.toJson(conn));
    }
}
