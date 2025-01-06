/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.reader.dolphinscheduler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.aliyun.dataworks.client.command.CommandApp;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.BatchExportProcessDefinitionByIdsRequest;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.PaginateData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.PaginateResponse;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.QueryProcessDefinitionByPaginateRequest;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DolphinschedulerApiV3Service;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.v320.DagDataSchedule;
import com.aliyun.migrationx.common.exception.BizException;
import com.aliyun.migrationx.common.exception.ErrorCode;
import com.aliyun.migrationx.common.utils.JSONUtils;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-07-19
 */
@Slf4j
public class DolphinSchedulerSingleJsonReader extends CommandApp {

    public static void main(String[] args) throws Exception {
        new DolphinSchedulerSingleJsonReader().run(args);
    }

    @Override
    public void run(String[] args) throws Exception {
        Options options = getOptions();
        CommandLine commandLine = getCommandLine(options, args);

        String endpoint = commandLine.getOptionValue("e");
        String token = commandLine.getOptionValue("t");
        String dolphinProjectCode = commandLine.getOptionValue("p");
        String targetFile = commandLine.getOptionValue("f");

        String[] projectCodeList = dolphinProjectCode.split(",");

        DolphinschedulerApiV3Service dolphinschedulerApiV3Service = new DolphinschedulerApiV3Service(endpoint, token);

        File file = new File(targetFile);
        if (file.exists() && !file.delete()) {
            log.error("target file exists and can not be deleted, file: {}", targetFile);
            throw new BizException(ErrorCode.NO_PERMISSION, "delete file " + targetFile);
        }
        try {
            BufferedWriter writer = new BufferedWriter(new java.io.FileWriter(file));
            AtomicBoolean isFirst = new AtomicBoolean(true);
            for (String projectCode : projectCodeList) {
                readDagDataSchedule(dolphinschedulerApiV3Service, projectCode, writer, isFirst);
            }
            writer.newLine();
            writer.write("]");
            writer.flush();
        } catch (IOException e) {
            log.error("write to target file error", e);
        }
        log.info("read dolphin scheduler success, dolphin scheduler json target path: {}", targetFile);
    }

    protected Options getOptions() {
        Options options = new Options();
        options.addRequiredOption("e", "endpoint", true, "dolphinscheduler endpoint, example: http://123.23.23.34:12345");
        options.addRequiredOption("t", "token", true, "dolphinscheduler token");
        options.addRequiredOption("p", "projectCode", true, "dolphinscheduler project code");
        options.addRequiredOption("f", "targetFile", true, "dolphinscheduler json target path");
        return options;
    }

    private void readDagDataSchedule(DolphinschedulerApiV3Service dolphinschedulerApiV3Service, String projectCode,
                                     BufferedWriter writer, AtomicBoolean isFirst) {
        int pageNo = 0;
        int pageSize = 10;
        int total = 0;
        QueryProcessDefinitionByPaginateRequest queryProcessDefinitionByPaginateRequest = new QueryProcessDefinitionByPaginateRequest();
        queryProcessDefinitionByPaginateRequest.setPageNo(pageNo);
        queryProcessDefinitionByPaginateRequest.setPageSize(pageSize);
        queryProcessDefinitionByPaginateRequest.setProjectCode(Long.valueOf(projectCode));

        BatchExportProcessDefinitionByIdsRequest batchExportProcessDefinitionByIdsRequest = new BatchExportProcessDefinitionByIdsRequest();
        batchExportProcessDefinitionByIdsRequest.setPageNo(1);
        batchExportProcessDefinitionByIdsRequest.setPageSize(Integer.MAX_VALUE);
        batchExportProcessDefinitionByIdsRequest.setProjectCode(Long.valueOf(projectCode));

        do {
            try {
                pageNo++;
                queryProcessDefinitionByPaginateRequest.setPageNo(pageNo);
                PaginateResponse<JsonObject> processDefinitionByPaging = dolphinschedulerApiV3Service.queryProcessDefinitionByPaging(
                    queryProcessDefinitionByPaginateRequest);
                PaginateData<JsonObject> data = processDefinitionByPaging.getData();
                total = data.getTotal();
                List<String> processCodeList = data.getTotalList().stream()
                    .filter(Objects::nonNull)
                    .map(jsonObject -> jsonObject.get("code"))
                    .filter(Objects::nonNull)
                    .map(JsonElement::getAsLong)
                    .map(String::valueOf)
                    .collect(Collectors.toList());
                batchExportProcessDefinitionByIdsRequest.setIds(processCodeList);
                String processDefinitionListJson = dolphinschedulerApiV3Service.batchExportProcessDefinitionByIds(
                    batchExportProcessDefinitionByIdsRequest);
                List<DagDataSchedule> dagDataScheduleList = JSONUtils.toList(processDefinitionListJson, DagDataSchedule.class);
                for (DagDataSchedule dagDataSchedule : dagDataScheduleList) {
                    if (isFirst.get()) {
                        writer.write("[");
                        isFirst.set(false);
                    } else {
                        writer.write(",");
                    }
                    writer.newLine();
                    writer.write(JSONUtils.toJsonString(dagDataSchedule));
                }
                writer.flush();
            } catch (Exception e) {
                log.error("read dolphin dag data error: {}", e.getLocalizedMessage(), e);
            }
        } while (pageNo * pageSize < total);
    }

}
