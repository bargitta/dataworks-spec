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

package com.aliyun.dataworks.migrationx.transformer.dataworks.transformer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import com.aliyun.dataworks.common.spec.utils.JSONUtils;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.DolphinSchedulerPackage;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.DolphinSchedulerVersion;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.service.DolphinSchedulerPackageFileService;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.Asset;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DataWorksPackage;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DwProject;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DwWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.types.AssetType;
import com.aliyun.dataworks.migrationx.domain.dataworks.service.impl.DataWorksDwmaPackageFileService;
import com.aliyun.dataworks.migrationx.domain.dataworks.service.impl.DataWorksSpecPackageFileService;
import com.aliyun.dataworks.migrationx.transformer.core.transformer.AbstractPackageTransformer;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.AbstractDolphinSchedulerConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v1.nodes.DolphinSchedulerV1Converter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v2.nodes.DolphinSchedulerV2Converter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.DolphinSchedulerV3Converter;
import com.aliyun.migrationx.common.context.TransformerContext;
import com.aliyun.migrationx.common.exception.BizException;
import com.aliyun.migrationx.common.exception.ErrorCode;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author 聿剑
 * @date 2023/02/15
 */
@SuppressWarnings("ALL")
@Slf4j
public class DataWorksDolphinSchedulerTransformer extends AbstractPackageTransformer<DolphinSchedulerPackage, DataWorksPackage> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataWorksDolphinSchedulerTransformer.class);
    protected File packageFile;
    protected DwProject dwProject;
    protected DataWorksTransformerConfig dataWorksTransformerConfig;
    protected Properties converterProperties;

    public DataWorksDolphinSchedulerTransformer(DataWorksTransformerConfig dataWorksTransformerConfig, DolphinSchedulerPackage sourcePacakgeFile,
            DataWorksPackage targetPackageFile) {
        super(null, sourcePacakgeFile, targetPackageFile);
        this.dataWorksTransformerConfig = dataWorksTransformerConfig;
    }

    @Override
    public void init() throws Exception {
        this.sourcePackageFileService = new DolphinSchedulerPackageFileService();

        initConfig();
        log.info("target package format: {}", this.dataWorksTransformerConfig.getFormat());

        switch (this.dataWorksTransformerConfig.getFormat()) {
            case DWMA:
                this.targetPackageFileService = new DataWorksDwmaPackageFileService();
                break;
            case SPEC:
            case WORKFLOW:
                this.targetPackageFileService = new DataWorksSpecPackageFileService();
                break;
            default:
                throw new RuntimeException(String.format("format % error", this.dataWorksTransformerConfig.getFormat()));
        }
        TransformerContext.getCollector().setTransformerType(dataWorksTransformerConfig.getFormat().name());
        TransformerContext.getContext().setSourceDir(sourcePackage.getPackageFile());
        this.targetPackageFileService.setLocale(this.dataWorksTransformerConfig.getLocale());
    }

    private void initConfig() throws IOException {
        this.dwProject = Optional.ofNullable(this.dataWorksTransformerConfig.getProject()).orElseGet(() -> {
            DwProject p = new DwProject();
            p.setName("tmp_transform_project");
            return p;
        });
        if (this.dwProject == null || StringUtils.isBlank(this.dwProject.getName())) {
            throw new BizException(ErrorCode.CONFIG_ITEM_INVALID).with("project.name").with("empty");
        }

        this.converterProperties = new Properties();
        Optional.ofNullable(this.dataWorksTransformerConfig).map(DataWorksTransformerConfig::getSettings).ifPresent(settings -> {
            settings.entrySet().stream().forEach(ent -> {
                if (ent.getValue() instanceof Map) {
                    this.converterProperties.put(ent.getKey(), JSONUtils.toJsonString(ent.getValue()));
                } else {
                    this.converterProperties.put(ent.getKey(), ent.getValue());
                }
            });
        });
        this.converterProperties.put("format", this.dataWorksTransformerConfig.getFormat());
    }

    @Override
    public void load() throws Exception {
        sourcePackageFileService.load(sourcePackage);
    }

    @Override
    public void transform() throws Exception {
        this.targetPackage.setDwProject(this.dwProject);
        this.packageFile = this.sourcePackage.getPackageFile();

        DolphinSchedulerVersion version = sourcePackageFileService.getPackage().getPackageInfo().getDolphinSchedulerVersion();
        AbstractDolphinSchedulerConverter schedulerConverter;
        switch (version) {
            case V1:
                schedulerConverter = new DolphinSchedulerV1Converter(sourcePackageFileService.getPackage());
                break;
            case V2:
                schedulerConverter = new DolphinSchedulerV2Converter(sourcePackageFileService.getPackage());
                break;
            case V3:
                schedulerConverter = new DolphinSchedulerV3Converter(sourcePackageFileService.getPackage());
                break;
            default:
                throw new RuntimeException("Unsupport version");
        }
        Asset asset = new Asset();
        asset.setType(AssetType.DW_EXPORT);
        schedulerConverter.setProject(this.dwProject);
        schedulerConverter.setProperties(this.converterProperties);
        List<DwWorkflow> workflowList = schedulerConverter.convert(asset);
        ListUtils.emptyIfNull(workflowList).stream().forEach(wf -> {
            wf.setProjectRef(this.dwProject);
        });
        this.dwProject.setWorkflows(new ArrayList<>(ListUtils.emptyIfNull(workflowList)));
    }

    @Override
    public void write() throws Exception {
        targetPackageFileService.write(targetPackage, targetPackage.getPackageFile());
    }
}
