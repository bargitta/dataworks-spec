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

package com.aliyun.dataworks.migrationx.writer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.aliyun.dataworks.client.command.CommandApp;
import com.aliyun.dataworks_public20200518.Client;
import com.aliyun.dataworks_public20200518.models.CreateImportMigrationAdvanceRequest;
import com.aliyun.dataworks_public20200518.models.CreateImportMigrationResponse;
import com.aliyun.dataworks_public20200518.models.GetMigrationProcessRequest;
import com.aliyun.dataworks_public20200518.models.GetMigrationProcessResponse;
import com.aliyun.dataworks_public20200518.models.GetMigrationSummaryRequest;
import com.aliyun.dataworks_public20200518.models.GetMigrationSummaryResponse;
import com.aliyun.dataworks_public20200518.models.StartMigrationRequest;
import com.aliyun.dataworks_public20200518.models.StartMigrationResponse;
import com.aliyun.dataworks_public20240518.models.GetJobStatusRequest;
import com.aliyun.dataworks_public20240518.models.GetJobStatusResponse;
import com.aliyun.dataworks_public20240518.models.ImportWorkflowDefinitionRequest;
import com.aliyun.dataworks_public20240518.models.ImportWorkflowDefinitionResponse;
import com.aliyun.dataworks_public20240518.models.ImportWorkflowDefinitionResponseBody;
import com.aliyun.migrationx.common.utils.DateUtils;
import com.aliyun.migrationx.common.utils.GsonUtils;
import com.aliyun.teaopenapi.models.Config;
import com.aliyun.teautil.models.RuntimeOptions;

import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * 调用DataWorks迁移助手OpenAPI进行包导入
 *
 * @author 聿剑
 * @date 2023/9/4
 */
@Slf4j
public class DataWorksMigrationAssistWriter extends CommandApp {
    private static final String IDE_URL_TEMPLATE = "https://ide2-{0}.data.aliyun.com/?defaultProjectId={1}";
    private static final String MIGRATION_REPORT_URL_TEMPLATE = "https://migration-{0}.data.aliyun.com/?defaultProjectId={1}#/import/{2}/report";

    private static final String DATA_WORKS_ENDPOINT = "dataworks.%s.aliyuncs.com";

    private static final int IMPORT_STATUS_CHECKING_WAIT_SECONDS = 15;
    private static final int IMPORT_STATUS_CHECKING_COUNT_THRESHOLD = 20;

    private static final String PACKAGE_TYPE = "SPEC";

    @Override
    public void run(String[] args) throws Exception {
        CommandLine commandLine = getCommandLine(getOptions(), args);
        String endpoint = commandLine.getOptionValue("e");
        String accessId = commandLine.getOptionValue("i");
        String accessKey = commandLine.getOptionValue("k");
        String regionId = commandLine.getOptionValue("r");
        String projectId = commandLine.getOptionValue("p");
        String file = commandLine.getOptionValue("f");
        String packageType = commandLine.getOptionValue("t");

        Config config = new Config();
        config.setAccessKeyId(accessId);
        config.setAccessKeySecret(accessKey);

        if (StringUtils.isNotBlank(regionId)) {
            config.setRegionId(regionId);
            if (endpoint == null) {
                endpoint = String.format(DATA_WORKS_ENDPOINT, regionId);
            }
        }
        config.setEndpoint(endpoint);
        if (endpoint == null && regionId == null) {
            throw new RuntimeException("no region or endpoint args");
        }
        log.info("do import with endpoint {}, region {}, projectId {}, file {}", endpoint, regionId, projectId, file);
        Path path = Paths.get(file);
        if (!Files.exists(path)) {
            throw new RuntimeException("file " + file + " not found");
        }
        if (packageType != null && "SPEC".equalsIgnoreCase(packageType)) {
            Client client = new Client(config);
            doImportNodes(file, projectId, regionId, client);
        } else {
            com.aliyun.dataworks_public20240518.Client client = new com.aliyun.dataworks_public20240518.Client(config);
            importWorkflow(path, projectId, client);
        }
    }

    private void importWorkflow(
            final Path dir, final String projectId, final com.aliyun.dataworks_public20240518.Client client
    ) throws Exception {
        log.info("Importing specifications under folder: {} to DataWorks Project Id: {}", dir, projectId);
        try (Stream<Path> specFiles = Files.list(dir)) {
            specFiles.forEachOrdered(specFile ->
            {
                if (Files.isDirectory(specFile)) {
                    try {
                        importWorkflow(specFile, projectId, client);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    importSingleFlowSpec(projectId, client, specFile);
                }
            });
        }
        log.info("Finished specifications importing, server may need sometime to analyze them.");
    }

    private boolean isJsonFile(Path path) {
        String fileName = path.getFileName().toString();
        return fileName.endsWith(".json");
    }

    void importSingleFlowSpec(String projectId, com.aliyun.dataworks_public20240518.Client client, Path specFile) {
        if (!isJsonFile(specFile) || Files.isDirectory(specFile)) {
            log.warn("**Invalid workflow definition file: {}, skip importing it.**", specFile);
            return;
        }
        log.info("Importing workflow definition from specification file: {}", specFile);
        try {
            String specContent = FileUtils.readFileToString(specFile.toFile(), StandardCharsets.UTF_8);
            ImportWorkflowDefinitionRequest request = new ImportWorkflowDefinitionRequest();
            request.setProjectId(projectId);
            request.setSpec(specContent);
            ImportWorkflowDefinitionResponse response = client.importWorkflowDefinition(request);
            log.info("importing workflow definition file : {} \n response: {}", specFile, GsonUtils.toJsonString(response));

            if (null == response
                    || null == response.getBody()
                    || null == response.getBody().getAsyncJob()) {
                throw new RuntimeException("workflow " + specFile.getFileName() + " response error ");
            }

            ImportWorkflowDefinitionResponseBody body = response.getBody();
            String requestId = body.getRequestId(), asyncJobId = body.getAsyncJob().getId();
            if (null != response.getHeaders() && response.getHeaders().containsKey("x-acs-trace-id")) {
                requestId = requestId + "(" + response.getHeaders().get("x-acs-trace-id") + ")";
            }
            log.info("Specification submitted successfully with request ids: {}", requestId);
            checkImportJobStatus(asyncJobId, client);
        } catch (Exception ex) {
            log.error("Failed importing {}, detailed exception is: ", specFile, ex);
        }
    }

    void checkImportJobStatus(String asyncJobId, com.aliyun.dataworks_public20240518.Client client) throws Exception {
        log.info("\tChecking workflow importing job {}'s status", asyncJobId);
        String jobStatus = "Fail";
        for (int i = 0; i < IMPORT_STATUS_CHECKING_COUNT_THRESHOLD; i++) {
            GetJobStatusRequest getJobStatusRequest = new GetJobStatusRequest();
            getJobStatusRequest.setJobId(asyncJobId);
            GetJobStatusResponse getJobStatusResponse = client.getJobStatus(getJobStatusRequest);
            jobStatus = getJobStatusResponse.getBody().getJobStatus().getStatus();
            log.info("\tWorkflow importing job {}'s latest status is: {}", asyncJobId, jobStatus);
            if ("Fail".equals(jobStatus)) {
                log.error("Importing job {} failed due to: {}", asyncJobId, getJobStatusResponse.getBody().getJobStatus().getError());
                break;
            } else if ("Success".equals(jobStatus)) {
                break;
            } else {
                TimeUnit.SECONDS.sleep(IMPORT_STATUS_CHECKING_WAIT_SECONDS);
            }
        }
        if ("Running".equals(jobStatus)) {
            log.warn("Exceeded job status checking threshold: {} seconds!! The workflow importing job [{}] is still under running.",
                    IMPORT_STATUS_CHECKING_COUNT_THRESHOLD * IMPORT_STATUS_CHECKING_WAIT_SECONDS, asyncJobId);
        } else {
            log.info("Importing job {}'s final status is: {}\n", asyncJobId, jobStatus);
        }
    }

    private void doImportNodes(String file, String projectId, String regionId, Client client) throws Exception {
        log.info("Importing file: {} to DataWorks Project Id: {}", file, projectId);
        CreateImportMigrationAdvanceRequest createRequest = new CreateImportMigrationAdvanceRequest();
        createRequest.setName("migrationx_import_" + projectId + System.currentTimeMillis());
        createRequest.setProjectId(Long.valueOf(projectId));
        createRequest.setPackageType(PACKAGE_TYPE);
        createRequest.setPackageFileObject(Files.newInputStream(Paths.get(file)));
        createRequest.setDescription("MigrationX import, Package file: " + file);
        //set any mapping info to disable engine replacement
        //Map<String, String> test = new HashMap<>();
        //test.put("test111", "test222");
        //String map = JSONUtils.toJsonString(test);
        //createRequest.setWorkspaceMap(map);

        RuntimeOptions runtime = new RuntimeOptions();
        CreateImportMigrationResponse createResponse = client.createImportMigrationAdvance(createRequest, runtime);
        log.info("CreateImportMigration Response: {}", GsonUtils.toJsonString(createResponse.getBody().getData()));

        if (!BooleanUtils.isTrue(createResponse.getBody().getSuccess())) {
            log.error("CreateImportMigration Error: {}", createResponse.getBody().getErrorMessage());
            System.exit(-1);
        }

        Long migrationId = createResponse.getBody().getData();
        log.info("StartMigration: {}", migrationId);
        StartMigrationRequest startRequest = new StartMigrationRequest();
        startRequest.setMigrationId(migrationId);
        startRequest.setProjectId(Long.valueOf(projectId));
        StartMigrationResponse startResponse = client.startMigration(startRequest);
        log.info("StartMigration Response: {}", GsonUtils.toJsonString(startResponse.getBody().getData()));
        if (!BooleanUtils.isTrue(createResponse.getBody().getSuccess())) {
            log.error("StartMigration Error: {}", startResponse.getBody().getErrorMessage());
            System.exit(-1);
        }

        waitForImportFinish(migrationId, client, Long.valueOf(projectId), regionId);
    }

    @SuppressWarnings("BusyWait")
    private static void waitForImportFinish(Long migrationId, Client client, Long projectId, String regionId) throws Exception {
        while (true) {
            Thread.sleep(5000L);

            GetMigrationSummaryRequest summaryReq = new GetMigrationSummaryRequest();
            summaryReq.setMigrationId(migrationId);
            summaryReq.setProjectId(projectId);
            GetMigrationSummaryResponse summaryResp = client.getMigrationSummary(summaryReq);
            log.info("GetMigrationSummary Response, migration task name: {}, status: {}, update time: {}",
                    summaryResp.getBody().getData().getName(), summaryResp.getBody().getData().getStatus(),
                    DateUtils.convertLongToDate(summaryResp.getBody().getData().getGmtModified()));

            GetMigrationProcessRequest progressReq = new GetMigrationProcessRequest();
            progressReq.setMigrationId(migrationId);
            progressReq.setProjectId(projectId);
            GetMigrationProcessResponse progressResp = client.getMigrationProcess(progressReq);
            String progressInfo = progressResp.getBody().getData().stream()
                    .map(st -> Joiner.on(" : ").join(st.getTaskName(), st.getTaskStatus()))
                    .collect(Collectors.joining(" | "));
            log.info("GetMigrationProgress Response: \n{}", progressInfo);

            String status = summaryResp.getBody().getData().getStatus();
            List<String> successStatus = Arrays.asList("IMPORT_SUCCESS", "PARTIAL_SUCCESS");
            List<String> failureStatus = Arrays.asList("IMPORT_ERROR", "REVOKED");
            if (CollectionUtils.union(successStatus, failureStatus).stream().anyMatch(st -> StringUtils.equalsIgnoreCase(st, status))) {
                if (successStatus.contains(status)) {
                    log.info("Migration {} success", migrationId);
                    log.info("Please refer to DataWorks Datastudio: {} to check the result", getIdeUrl(regionId, projectId));
                } else {
                    log.error("Migration {} failed with status: {}", migrationId, status);
                    log.info("Please refer to DataWorks Migration: {} to check the result", getMigrationUrl(regionId, projectId, migrationId));
                }
                break;
            }
        }
    }

    private static String getMigrationUrl(String regionId, Long projectId, Long migrationId) {
        return MessageFormat.format(MIGRATION_REPORT_URL_TEMPLATE, regionId, String.valueOf(projectId), String.valueOf(migrationId));
    }

    private static String getIdeUrl(String regionId, Long projectId) {
        return MessageFormat.format(IDE_URL_TEMPLATE, regionId, String.valueOf(projectId));
    }

    protected Options getOptions() {
        Options options = new Options();
        options.addOption("e", "endpoint", true,
                "DataWorks OpenAPI endpoint, example: http://dataworks.cn-shanghai.aliyuncs.com");
        options.addRequiredOption("i", "accessKeyId", true, "Access key id");
        options.addRequiredOption("k", "accessKey", true, "Access key secret");
        options.addRequiredOption("r", "regionId", true, "Region id, example: cn-shanghai");
        options.addRequiredOption("p", "projectId", true, "DataWorks Project ID");
        options.addRequiredOption("f", "file", true, "Import Package file");
        options.addOption("t", "packageType", true, "Import Package type, example: SPEC, DATAWORKS_MODEL");
        return options;
    }
}
