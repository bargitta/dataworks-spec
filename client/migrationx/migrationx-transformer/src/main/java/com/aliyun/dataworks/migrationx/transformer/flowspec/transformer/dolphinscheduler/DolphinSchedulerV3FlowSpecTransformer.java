/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.flowspec.transformer.dolphinscheduler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.SpecUtil;
import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.v320.DagDataSchedule;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.DolphinSchedulerV3FlowSpecConverter;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.common.context.DolphinSchedulerV3ConverterContext;
import com.aliyun.dataworks.migrationx.transformer.flowspec.model.SpecRefEntityWrapper;
import com.aliyun.dataworks.migrationx.transformer.flowspec.transformer.AbstractTransformer;
import com.aliyun.migrationx.common.exception.BizException;
import com.aliyun.migrationx.common.exception.ErrorCode;
import com.aliyun.migrationx.common.utils.JSONUtils;
import com.aliyun.migrationx.common.utils.JsonFileUtils;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-06-27
 */
@Slf4j
public class DolphinSchedulerV3FlowSpecTransformer extends AbstractTransformer {

    protected final List<DagDataSchedule> dagDataScheduleList;

    protected final List<Specification<DataWorksWorkflowSpec>> specificationList;

    protected DolphinSchedulerV3ConverterContext context;

    public DolphinSchedulerV3FlowSpecTransformer(String configPath, String sourcePath, String targetPath) {
        this.configPath = configPath;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;

        dagDataScheduleList = new ArrayList<>();
        specificationList = new ArrayList<>();
    }

    /**
     * transform entry point
     */
    @Override
    public void transform() {
        // read process info from file
        read();
        // transform dolphin process to dataworks workflow
        doTransform();
        // write workflow to file
        write();
    }

    private void read() {
        try {
            context = parseContext();
            JsonParser jsonParser = JsonFileUtils.buildJsonParser(Files.newInputStream(Paths.get(sourcePath)));
            JsonNode jsonNode;
            while ((jsonNode = JSONUtils.readObjFromParser(jsonParser)) != null) {
                dagDataScheduleList.add(JSONUtils.parseObject(jsonNode, DagDataSchedule.class));
            }
        } catch (IOException e) {
            log.error("read config or source file error", e);
            throw new RuntimeException(e);
        }
    }

    private void doTransform() {
        // workflows in config file
        ListUtils.emptyIfNull(context.getDependSpecification()).stream()
            .map(Specification::getSpec)
            .filter(Objects::nonNull)
            .map(DataWorksWorkflowSpec::getWorkflows)
            .filter(Objects::nonNull)
            .flatMap(List::stream)
            .filter(workflow -> StringUtils.isNotBlank(workflow.getId()))
            .forEach(workflow -> {
                context.getSpecRefEntityMap().put(workflow.getId(), new SpecRefEntityWrapper().setSpecRefEntity(workflow));
            });

        specificationList.addAll(ListUtils.emptyIfNull(dagDataScheduleList).stream()
            .map(dagDataSchedule -> new DolphinSchedulerV3FlowSpecConverter(dagDataSchedule, context).convert())
            .flatMap(Collection::stream)
            .peek(specification -> log.info("specification: {}", specification))
            .collect(Collectors.toList()));
    }

    private void write() {
        File targetFile = new File(targetPath);
        if (targetFile.exists() && !targetFile.delete()) {
            log.error("target file exists and can not be deleted, file: {}", targetFile);
            throw new BizException(ErrorCode.NO_PERMISSION, "delete file " + targetFile);
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(targetFile, true))) {
            writer.write("[");
            for (int i = 0; i < specificationList.size(); i++) {
                writer.write(SpecUtil.writeToSpec(specificationList.get(i)));
                if (i != specificationList.size() - 1) {
                    writer.write(",");
                }
            }
            writer.write("]");
        } catch (IOException e) {
            log.error("write to target file error", e);
            throw new RuntimeException(e);
        }
    }

    private DolphinSchedulerV3ConverterContext parseContext() {
        try {
            String content = FileUtils.readFileToString(new File(configPath), StandardCharsets.UTF_8);
            DolphinSchedulerV3FlowSpecTransformerConfig config = JSONUtils.parseObject(content, DolphinSchedulerV3FlowSpecTransformerConfig.class);
            return Optional.ofNullable(config).map(DolphinSchedulerV3FlowSpecTransformerConfig::getContext).orElseThrow(
                () -> new BizException(ErrorCode.PARSE_CONFIG_FILE_FAILED, config));
        } catch (IOException e) {
            log.error("read config file error", e);
            throw new RuntimeException(e);
        }
    }
}
