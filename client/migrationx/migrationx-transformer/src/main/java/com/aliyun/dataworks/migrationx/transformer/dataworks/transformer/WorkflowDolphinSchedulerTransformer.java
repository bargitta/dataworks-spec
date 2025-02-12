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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import com.aliyun.dataworks.common.spec.SpecUtil;
import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.DolphinSchedulerPackage;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.DolphinSchedulerVersion;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DataWorksPackage;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v1.workflow.DolphinSchedulerV1WorkflowConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v2.workflow.DolphinSchedulerV2WorkflowConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.DolphinSchedulerV3WorkflowConverter;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

@Slf4j
public class WorkflowDolphinSchedulerTransformer extends DataWorksDolphinSchedulerTransformer {

    public WorkflowDolphinSchedulerTransformer(DataWorksTransformerConfig config, DolphinSchedulerPackage sourcePacakgeFile, DataWorksPackage targetPackageFile) {
        super(config, sourcePacakgeFile, targetPackageFile);
    }

    private List<Specification<DataWorksWorkflowSpec>> specifications;

    @Override
    public void transform() throws Exception {
        this.targetPackage.setDwProject(this.dwProject);
        this.packageFile = this.sourcePackage.getPackageFile();

        DolphinSchedulerVersion version = sourcePackageFileService.getPackage().getPackageInfo().getDolphinSchedulerVersion();
        switch (version) {
            case V1:
                DolphinSchedulerV1WorkflowConverter v1Converter =
                        new DolphinSchedulerV1WorkflowConverter(sourcePackageFileService.getPackage(), this.converterProperties);
                specifications = v1Converter.convert();
                break;
            case V2:
                DolphinSchedulerV2WorkflowConverter v2Converter =
                        new DolphinSchedulerV2WorkflowConverter(sourcePackageFileService.getPackage(), this.converterProperties);
                specifications = v2Converter.convert();
                break;
            case V3:
                DolphinSchedulerV3WorkflowConverter v3Converter =
                        new DolphinSchedulerV3WorkflowConverter(sourcePackageFileService.getPackage(), this.converterProperties);
                specifications = v3Converter.convert();
                break;
            default:
                throw new RuntimeException("Unsupported version");
        }
    }

    @Override
    public void write() throws Exception {
        File target = targetPackage.getPackageFile();
        doWrite(target);
    }

    public void doWrite(File target) {
        log.info("write workflow package to {}", target.getAbsolutePath());
        //clear files
        clearDir(target);

        for (Specification<DataWorksWorkflowSpec> specification : specifications) {
            DataWorksWorkflowSpec spec = specification.getSpec();
            for (SpecWorkflow workflow : spec.getWorkflows()) {
                String workflowName = workflow.getName();
                File workflowFile = new File(target, workflowName);
                if (!workflowFile.exists()) {
                    workflowFile.mkdir();
                }
                File file = new File(workflowFile, specification.getSpec().getName() + ".json");
                try {
                    Files.write(file.toPath(), SpecUtil.writeToSpec(specification).getBytes(StandardCharsets.UTF_8));
                } catch (Exception e) {
                    log.error("write to target file error", e);
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void clearDir(File target) {
        if (target.exists()) {
            try {
                FileUtils.deleteDirectory(target);
            } catch (Exception e) {
                throw new RuntimeException("delete file " + target.getAbsolutePath() + " failed");
            }
        }
        target.mkdirs();
    }
}
