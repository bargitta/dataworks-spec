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

package com.aliyun.dataworks.migrationx.transformer.dataworks.apps;

import java.io.File;
import java.nio.charset.StandardCharsets;

import com.aliyun.dataworks.common.spec.utils.JSONUtils;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.DolphinSchedulerPackage;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DataWorksPackage;
import com.aliyun.dataworks.migrationx.domain.dataworks.standard.objects.Package;
import com.aliyun.dataworks.migrationx.transformer.core.BaseTransformerApp;
import com.aliyun.dataworks.migrationx.transformer.core.transformer.Transformer;
import com.aliyun.dataworks.migrationx.transformer.dataworks.transformer.DataWorksDolphinSchedulerTransformer;
import com.aliyun.dataworks.migrationx.transformer.dataworks.transformer.DataWorksPackageFormat;
import com.aliyun.dataworks.migrationx.transformer.dataworks.transformer.DataWorksTransformerConfig;
import com.aliyun.dataworks.migrationx.transformer.dataworks.transformer.WorkflowDolphinSchedulerTransformer;
import com.aliyun.migrationx.common.context.TransformerContext;
import com.aliyun.migrationx.common.exception.BizException;
import com.aliyun.migrationx.common.exception.ErrorCode;
import com.aliyun.migrationx.common.metrics.enums.CollectorType;
import com.aliyun.migrationx.common.utils.Config;
import com.aliyun.migrationx.common.utils.GsonUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

/**
 * @author 聿剑
 * @date 2023/02/10
 */
@Slf4j
public class DataWorksDolphinschedulerTransformerApp extends BaseTransformerApp {

    public DataWorksDolphinschedulerTransformerApp() {
        super(DolphinSchedulerPackage.class, DataWorksPackage.class);
    }

    @Override
    public void initCollector() {
        TransformerContext.init(CollectorType.DolphinScheduler);
        super.initCollector();
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected Transformer createTransformer(File configFile, Package from, Package to) {
        DolphinSchedulerPackage dolphinSchedulerPackage = (DolphinSchedulerPackage) from;
        DataWorksPackage dataWorksPackage = (DataWorksPackage) to;
        DataWorksTransformerConfig config = initConfig(configFile);
        log.info("dataworks transformer configFile: {} config: {}", configFile.getAbsolutePath(), config);
        if (config.getFormat() != null && DataWorksPackageFormat.WORKFLOW.equals(config.getFormat())) {
            return new WorkflowDolphinSchedulerTransformer(config, dolphinSchedulerPackage, dataWorksPackage);
        } else {
            return new DataWorksDolphinSchedulerTransformer(config, dolphinSchedulerPackage, dataWorksPackage);
        }
    }

    private DataWorksTransformerConfig initConfig(File configFile) {
        if (!configFile.exists()) {
            log.error("config file not exists: {}", configFile);
            throw new RuntimeException("file not found by " + configFile.getAbsolutePath());
        }
        try {
            String config = FileUtils.readFileToString(configFile, StandardCharsets.UTF_8);
            DataWorksTransformerConfig dataWorksTransformerConfig
                    = JSONUtils.parseObject(config, new TypeReference<DataWorksTransformerConfig>() {});
            if (dataWorksTransformerConfig == null) {
                log.error("config file: {}, config class: {}", configFile, DataWorksTransformerConfig.class);
                throw new BizException(ErrorCode.PARSE_CONFIG_FILE_FAILED).with(configFile);
            }
            Config dwConfig = GsonUtils.fromJsonString(config, new TypeToken<Config>() {}.getType());
            Config.init(dwConfig);
            log.info("config replaceMapping {}", JSONUtils.toJsonString(Config.get().getReplaceMapping()));
            return dataWorksTransformerConfig;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
