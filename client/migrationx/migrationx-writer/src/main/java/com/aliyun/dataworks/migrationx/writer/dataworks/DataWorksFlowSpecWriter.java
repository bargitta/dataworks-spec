package com.aliyun.dataworks.migrationx.writer.dataworks;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import com.aliyun.dataworks.client.command.CommandApp;
import com.aliyun.dataworks.common.spec.SpecUtil;
import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.SpecEntity;
import com.aliyun.dataworks.common.spec.domain.SpecRefEntity;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.interfaces.LabelEnum;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.service.openapi.DataWorksOpenApiService;
import com.aliyun.dataworks.migrationx.writer.dataworks.model.AsyncJobStatus;
import com.aliyun.dataworks_public20240518.models.GetJobStatusResponseBody.GetJobStatusResponseBodyJobStatus;
import com.aliyun.migrationx.common.utils.JSONUtils;
import com.aliyun.migrationx.common.utils.JsonFileUtils;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.collections4.CollectionUtils;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-10-10
 */
@Slf4j
public class DataWorksFlowSpecWriter extends CommandApp {

    private static final int MAX_LOOP = 10;

    public static void main(String[] args) throws Exception {
        new DataWorksFlowSpecWriter().run(args);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addRequiredOption("r", "region", true, "dataworks region, like cn-shanghai");
        options.addRequiredOption("p", "projectId", true, "dataworks project id");
        options.addRequiredOption("f", "file", true, "flow spec json file");
        // dataworks auth, using env variable is supported.
        options.addOption("i", "accessKeyId", true, "dataworks access key id");
        options.addOption("s", "accessKeySecret", true, "dataworks access key secret");
        return options;
    }

    @Override
    protected void doCommandRun(Options options, CommandLine cli, String[] args) {
        String region = cli.getOptionValue("r");
        String projectId = cli.getOptionValue("p");
        String filePath = cli.getOptionValue("f");
        String accessKeyId = cli.getOptionValue("i");
        String accessKeySecret = cli.getOptionValue("s");

        doSubmitFile(region, projectId, accessKeyId, accessKeySecret, filePath);
    }

    public void doSubmitFile(String regionId, String projectId, String accessKeyId, String accessKeySecret, String filePath) {
        DataWorksOpenApiService dataWorksOpenApiService = new DataWorksOpenApiService(regionId, projectId, accessKeyId, accessKeySecret);

        File file = new File(filePath);
        if (!file.exists()) {
            log.error("file not found, file path: {}", filePath);
            throw new IllegalArgumentException("file not found, file path: " + filePath);
        }
        try {
            batchWriteFlow(dataWorksOpenApiService, projectId, Files.newInputStream(file.toPath()));
        } catch (Exception e) {
            log.error("batch write flow failed, error: {}", e.getMessage());
        }
    }

    public void batchWriteFlow(DataWorksOpenApiService dataWorksOpenApiService, String projectId, InputStream inputStream) {
        Set<String> asyncJobIdSet = new HashSet<>();
        // batch submit flow spec import task
        JsonParser jsonParser = JsonFileUtils.buildJsonParser(inputStream);
        JsonNode jsonNode;
        while ((jsonNode = JSONUtils.readObjFromParser(jsonParser)) != null) {
            String flowSpec = JSONUtils.toJsonString(jsonNode);
            Specification<DataWorksWorkflowSpec> specSpecification = SpecUtil.parseToDomain(flowSpec);
            if (SpecKind.NODE.getLabel().equalsIgnoreCase(specSpecification.getKind())) {
                // save node
                String uuid = dataWorksOpenApiService.saveNode(projectId,
                    "DataworksProject", getSpecId(specSpecification), null, flowSpec);
                if (uuid == null) {
                    log.error("write node error, node spec: {}", flowSpec);
                }
            } else {
                // save workflow and inner node
                String asyncJobId = dataWorksOpenApiService.importWorkflow(projectId, flowSpec);
                asyncJobIdSet.add(asyncJobId);
            }
        }
        // check save workflow async job status
        checkAsyncJobStatus(dataWorksOpenApiService, asyncJobIdSet);
    }

    private void checkAsyncJobStatus(DataWorksOpenApiService dataWorksOpenApiService, Set<String> asyncJobIdSet) {
        Set<String> successSet = new HashSet<>();
        Set<String> failureSet = new HashSet<>();
        for (int i = 0; i < MAX_LOOP; i++) {
            asyncJobIdSet.forEach(asyncJobId -> {
                AsyncJobStatus asyncJobStatus = getAsyncJobStatus(dataWorksOpenApiService, asyncJobId);
                if (asyncJobStatus != null && asyncJobStatus.getCompleted()) {
                    switch (asyncJobStatus) {
                        case SUCCESS:
                            successSet.add(asyncJobId);
                            break;
                        case FAIL:
                            failureSet.add(asyncJobId);
                            break;
                        default:
                    }
                    log.info("async job finish, jobId: {}, status: {}", asyncJobId, asyncJobStatus.getLabel());
                }
            });
            asyncJobIdSet.removeAll(successSet);
            asyncJobIdSet.removeAll(failureSet);
            if (asyncJobIdSet.isEmpty()) {
                break;
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                log.warn("write thread is interrupted, not sure whether the write is successful");
                return;
            }
        }
        // log failure async job
        if (CollectionUtils.isNotEmpty(failureSet)) {
            log.error("async job run error, ids: {}", failureSet);
        }
        // there are unfinished async job, need log them
        if (CollectionUtils.isNotEmpty(asyncJobIdSet)) {
            log.warn("async job not finish after {} seconds, async job id: {}", MAX_LOOP * 3, asyncJobIdSet);
        }
    }

    private AsyncJobStatus getAsyncJobStatus(DataWorksOpenApiService dataWorksOpenApiService, String asyncJobId) {
        GetJobStatusResponseBodyJobStatus asyncJob = dataWorksOpenApiService.getAsyncJob(asyncJobId);
        if (asyncJob == null || asyncJob.getStatus() == null) {
            return null;
        }
        return LabelEnum.getByLabel(AsyncJobStatus.class, asyncJob.getStatus());
    }

    private String getSpecId(Specification<DataWorksWorkflowSpec> specification) {
        String uuid = Optional.ofNullable(specification)
            .map(SpecEntity::getMetadata)
            .map(metadata -> metadata.get("uuid"))
            .filter(String.class::isInstance)
            .map(String.class::cast)
            .orElse(null);
        if (uuid != null) {
            return uuid;
        }

        SpecKind specKind = Optional.ofNullable(specification)
            .map(Specification::getKind)
            .map(kind -> LabelEnum.getByLabel(SpecKind.class, kind))
            .orElse(null);
        if (specKind == null) {
            return null;
        }
        switch (specKind) {
            case NODE:
                return Optional.of(specification)
                    .map(Specification::getSpec)
                    .map(DataWorksWorkflowSpec::getNodes)
                    .orElse(Collections.emptyList()).stream()
                    .findFirst()
                    .map(SpecNode::getId)
                    .orElse(null);
            case CYCLE_WORKFLOW:
            case MANUAL_WORKFLOW:
                return Optional.of(specification)
                    .map(Specification::getSpec)
                    .map(DataWorksWorkflowSpec::getWorkflows)
                    .orElse(Collections.emptyList()).stream()
                    .findFirst()
                    .map(SpecWorkflow::getId)
                    .orElse(null);
            default:
                return Optional.of(specification)
                    .map(Specification::getSpec)
                    .map(SpecRefEntity::getId)
                    .orElse(null);
        }
    }
}
