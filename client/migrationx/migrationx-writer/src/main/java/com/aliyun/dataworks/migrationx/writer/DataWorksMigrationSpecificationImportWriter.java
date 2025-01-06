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
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.aliyun.dataworks_public20240518.Client;
import com.aliyun.dataworks_public20240518.models.GetJobStatusRequest;
import com.aliyun.dataworks_public20240518.models.GetJobStatusResponse;
import com.aliyun.dataworks_public20240518.models.ImportWorkflowDefinitionRequest;
import com.aliyun.dataworks_public20240518.models.ImportWorkflowDefinitionResponse;
import com.aliyun.dataworks_public20240518.models.ImportWorkflowDefinitionResponseBody;
import com.aliyun.dataworks.client.command.CommandApp;
import com.aliyun.migrationx.common.utils.GsonUtils;
import com.aliyun.teaopenapi.models.Config;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class DataWorksMigrationSpecificationImportWriter extends CommandApp {
    private static final int IMPORT_STATUS_CHECKING_WAIT_SECONDS = 15;
    private static final int IMPORT_STATUS_CHECKING_COUNT_THRESHOLD = 20;
    private static final String DATA_WORKS_ENDPOINT = "dataworks.%s.aliyuncs.com";

    @Override
    public void run(String[] args) throws Exception {
        CommandLine commandLine = getCommandLine(getOptions(), args);
        String endpoint = commandLine.getOptionValue("e");
        String accessId = commandLine.getOptionValue("i");
        String accessKey = commandLine.getOptionValue("k");
        String regionId = commandLine.getOptionValue("r");
        String projectId = commandLine.getOptionValue("p");
        String flowSpecFolder = commandLine.getOptionValue("f");

        Config config = new Config().setAccessKeyId(accessId).setAccessKeySecret(accessKey);
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
        log.info("do import with endpoint {}, region {}, projectId {}, spec folder {}", endpoint, regionId, projectId, flowSpecFolder);
        Client client = new Client(config);
        doImport(flowSpecFolder, projectId, client);
    }

    private void doImport(
            final String flowSpecFolder, final String projectId, final Client client
    ) throws Exception {
        log.info("Importing specifications under folder: {} to DataWorks Project Id: {}", flowSpecFolder, projectId);
        try (Stream<Path> specFiles = Files.list(Paths.get(flowSpecFolder))) {
            specFiles.forEachOrdered(specFile -> importSingleFlowSpec(projectId, client, specFile));
        }
        log.info("Finished specifications importing, server may need sometime to analyze them.");
    }

    private boolean isJsonFile(Path path) {
        String fileName = path.getFileName().toString();
        return fileName.endsWith(".json");
    }

    void importSingleFlowSpec(String projectId, Client client, Path specFile) {
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
            if (null == response
                    || null == response.getStatusCode()
                    || 200 != response.getStatusCode()
                    || null == response.getBody()
                    || null == response.getBody().getAsyncJob()) {
                log.error("Failed importing workflow definition: {}, server replied: {}", specFile, GsonUtils.toJsonString(response));
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

    void checkImportJobStatus(String asyncJobId, Client client) throws Exception {
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

    protected Options getOptions() {
        return new Options().addOption("e", "endpoint", true,
                        "DataWorks OpenAPI endpoint, example: http://dataworks.cn-shanghai.aliyuncs.com")
                .addRequiredOption("i", "accessKeyId", true, "Access key id")
                .addRequiredOption("k", "accessKey", true, "Access key secret")
                .addRequiredOption("r", "regionId", true, "Region id, example: cn-shanghai")
                .addRequiredOption("p", "projectId", true, "DataWorks Project ID")
                .addRequiredOption("f", "flowspecFolder", true, "Flowspec file folder");
    }
}
