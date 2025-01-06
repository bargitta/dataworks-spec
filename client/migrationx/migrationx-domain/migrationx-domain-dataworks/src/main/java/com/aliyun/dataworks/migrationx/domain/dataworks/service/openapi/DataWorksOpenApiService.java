/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.domain.dataworks.service.openapi;

import java.util.Optional;

import com.aliyun.dataworks_public20240518.Client;
import com.aliyun.dataworks_public20240518.models.CreateNodeRequest;
import com.aliyun.dataworks_public20240518.models.CreateNodeResponse;
import com.aliyun.dataworks_public20240518.models.CreateNodeResponseBody;
import com.aliyun.dataworks_public20240518.models.CreateWorkflowDefinitionRequest;
import com.aliyun.dataworks_public20240518.models.CreateWorkflowDefinitionResponse;
import com.aliyun.dataworks_public20240518.models.CreateWorkflowDefinitionResponseBody;
import com.aliyun.dataworks_public20240518.models.GetJobStatusRequest;
import com.aliyun.dataworks_public20240518.models.GetJobStatusResponse;
import com.aliyun.dataworks_public20240518.models.GetJobStatusResponseBody;
import com.aliyun.dataworks_public20240518.models.GetJobStatusResponseBody.GetJobStatusResponseBodyJobStatus;
import com.aliyun.dataworks_public20240518.models.GetNodeRequest;
import com.aliyun.dataworks_public20240518.models.GetNodeResponse;
import com.aliyun.dataworks_public20240518.models.GetNodeResponseBody;
import com.aliyun.dataworks_public20240518.models.GetNodeResponseBody.GetNodeResponseBodyNode;
import com.aliyun.dataworks_public20240518.models.GetWorkflowDefinitionRequest;
import com.aliyun.dataworks_public20240518.models.GetWorkflowDefinitionResponse;
import com.aliyun.dataworks_public20240518.models.GetWorkflowDefinitionResponseBody;
import com.aliyun.dataworks_public20240518.models.GetWorkflowDefinitionResponseBody.GetWorkflowDefinitionResponseBodyWorkflowDefinition;
import com.aliyun.dataworks_public20240518.models.ImportWorkflowDefinitionRequest;
import com.aliyun.dataworks_public20240518.models.ImportWorkflowDefinitionResponse;
import com.aliyun.dataworks_public20240518.models.ImportWorkflowDefinitionResponseBody;
import com.aliyun.dataworks_public20240518.models.ImportWorkflowDefinitionResponseBody.ImportWorkflowDefinitionResponseBodyAsyncJob;
import com.aliyun.dataworks_public20240518.models.ListNodeDependenciesRequest;
import com.aliyun.dataworks_public20240518.models.ListNodeDependenciesResponse;
import com.aliyun.dataworks_public20240518.models.ListNodeDependenciesResponseBody;
import com.aliyun.dataworks_public20240518.models.ListNodeDependenciesResponseBody.ListNodeDependenciesResponseBodyPagingInfo;
import com.aliyun.dataworks_public20240518.models.ListWorkflowDefinitionsRequest;
import com.aliyun.dataworks_public20240518.models.ListWorkflowDefinitionsResponse;
import com.aliyun.dataworks_public20240518.models.ListWorkflowDefinitionsResponseBody;
import com.aliyun.dataworks_public20240518.models.ListWorkflowDefinitionsResponseBody.ListWorkflowDefinitionsResponseBodyPagingInfo;
import com.aliyun.dataworks_public20240518.models.UpdateNodeRequest;
import com.aliyun.dataworks_public20240518.models.UpdateNodeResponse;
import com.aliyun.dataworks_public20240518.models.UpdateNodeResponseBody;
import com.aliyun.dataworks_public20240518.models.UpdateWorkflowDefinitionRequest;
import com.aliyun.dataworks_public20240518.models.UpdateWorkflowDefinitionResponse;
import com.aliyun.dataworks_public20240518.models.UpdateWorkflowDefinitionResponseBody;
import com.aliyun.migrationx.common.utils.JSONUtils;
import com.aliyun.teaopenapi.models.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-09-20
 */
@Slf4j
public class DataWorksOpenApiService {

    private static final String SYSTEM_ENV_ACCESS_KEY_ID = "ALIBABA_CLOUD_ACCESS_KEY_ID";

    private static final String SYSTEM_ENV_ACCESS_KEY_SECRET = "ALIBABA_CLOUD_ACCESS_KEY_SECRET";

    private static final int SUCCESS_STATUS = 200;

    private static final String endpoint = "dataworks.%s.aliyuncs.com";

    private final String regionId;

    private final String projectId;

    private final String accessKeyId;

    private final String accessKeySecret;

    private Client client;

    private boolean initialized;

    public DataWorksOpenApiService(String region, String projectId, String accessKeyId, String accessKeySecret) {
        if (StringUtils.isBlank(region) || StringUtils.isBlank(projectId)) {
            throw new RuntimeException("region or projectId is blank");
        }
        this.projectId = projectId;
        this.regionId = region;
        this.accessKeyId = Optional.ofNullable(accessKeyId).orElseGet(() -> System.getenv(SYSTEM_ENV_ACCESS_KEY_ID));
        this.accessKeySecret = Optional.ofNullable(accessKeySecret).orElseGet(() -> System.getenv(SYSTEM_ENV_ACCESS_KEY_SECRET));
        initClient();
    }

    /**
     * init client
     *
     * @return is or not initialized
     */
    public boolean initClient() {
        if (initialized) {
            return true;
        }
        if (StringUtils.isAnyBlank(projectId, accessKeyId, accessKeySecret)) {
            throw new RuntimeException("endpoint or projectId or accessKeyId or accessKeySecret is blank");
        }
        Config config = new Config()
                .setAccessKeyId(accessKeyId)
                .setAccessKeySecret(accessKeySecret)
                .setEndpoint(String.format(endpoint,regionId))
                .setRegionId(regionId);
        try {
            this.client = new Client(config);
            initialized = true;
        } catch (Exception e) {
            log.error("init dataworks client failed", e);
            return false;
        }
        return true;
    }

    private void checkInit() {
        if (!initialized && !initClient()) {
            throw new RuntimeException("dataworks client is not initialized");
        }
    }

    /**
     * save node, this is an idempotent interface when you fill in the id field.
     *
     * @param projectId   projectId
     * @param scene       scene of node
     * @param uuid        node preset uuid or uuid for update, if not exist, will create a new node
     * @param containerId containerId
     * @param spec        spec
     * @return node id
     */
    public String saveNode(String projectId, String scene, String uuid, String containerId, String spec) {
        if (nodeExist(projectId, uuid)) {
            UpdateNodeRequest updateNodeRequest = new UpdateNodeRequest()
                    .setProjectId(projectId)
                    .setId(uuid)
                    .setSpec(spec);
            return updateNode(updateNodeRequest) ? uuid : null;
        } else {
            CreateNodeRequest createNodeRequest = new CreateNodeRequest()
                    .setProjectId(projectId)
                    .setScene(scene)
                    .setContainerId(containerId)
                    .setSpec(spec);
            return createNode(createNodeRequest);
        }
    }

    /**
     * save workflow, this is an idempotent interface when you fill in the id field.
     * but just create a new workflow, don't deal inner node
     *
     * @param projectId projectId
     * @param uuid      uuid
     * @param spec      spec
     * @return workflow id
     */
    public String saveWorkflow(String projectId, String uuid, String spec) {
        if (workflowExist(projectId, uuid)) {
            UpdateWorkflowDefinitionRequest request = new UpdateWorkflowDefinitionRequest()
                    .setProjectId(projectId)
                    .setId(uuid)
                    .setSpec(spec);
            return updateWorkflow(request) ? uuid : null;
        } else {
            CreateWorkflowDefinitionRequest request = new CreateWorkflowDefinitionRequest()
                    .setProjectId(projectId)
                    .setSpec(spec);
            return createWorkflow(request);
        }
    }

    /**
     * import workflow with inner node to dataworks.
     * the interface is asynchronous, so the return value is async job id instead of workflow id
     *
     * @param projectId project id
     * @param spec      workflow spec
     * @return async id
     */
    public String importWorkflow(String projectId, String spec) {
        checkInit();
        ImportWorkflowDefinitionRequest importWorkflowDefinitionRequest = new ImportWorkflowDefinitionRequest()
                .setProjectId(projectId)
                .setSpec(spec);
        log.info("import flow request: {}", importWorkflowDefinitionRequest);
        ImportWorkflowDefinitionResponse response;
        try {
            response = client.importWorkflowDefinition(importWorkflowDefinitionRequest);
        } catch (Exception e) {
            log.error("import flow failed", e);
            throw new RuntimeException(e);
        }
        if (response == null) {
            throw new RuntimeException("response is null");
        }
        if (response.getStatusCode() != null && SUCCESS_STATUS != response.getStatusCode()) {
            log.error("import flow failed, requestId: {}", response.getBody().getRequestId());
            throw new RuntimeException("import flow failed");
        }
        log.info("import flow async job submit success, response: {}", JSONUtils.toJsonString(response));
        return Optional.of(response)
                .map(ImportWorkflowDefinitionResponse::getBody)
                .map(ImportWorkflowDefinitionResponseBody::getAsyncJob)
                .map(ImportWorkflowDefinitionResponseBodyAsyncJob::getId)
                .orElse(null);
    }

    /**
     * get async job result
     *
     * @param jobId async job id
     * @return async job info
     */
    public GetJobStatusResponseBodyJobStatus getAsyncJob(String jobId) {
        checkInit();
        GetJobStatusRequest getJobStatusRequest = new GetJobStatusRequest()
                .setJobId(jobId);
        log.info("get async job request: {}", getJobStatusRequest);
        GetJobStatusResponse response;
        try {
            response = client.getJobStatus(getJobStatusRequest);
        } catch (Exception e) {
            log.error("get async job failed", e);
            throw new RuntimeException(e);
        }
        if (response == null) {
            throw new RuntimeException("response is null");
        }
        if (response.getStatusCode() != null && SUCCESS_STATUS != response.getStatusCode()) {
            log.error("get async job failed, requestId: {}", response.getBody().getRequestId());
            throw new RuntimeException("get async job failed");
        }
        log.info("get async job success, response: {}", JSONUtils.toJsonString(response));
        return Optional.of(response)
                .map(GetJobStatusResponse::getBody)
                .map(GetJobStatusResponseBody::getJobStatus)
                .orElse(null);
    }

    /**
     * create node
     *
     * @param createNodeRequest create node request
     * @return node id
     */
    public String createNode(CreateNodeRequest createNodeRequest) {
        checkInit();
        log.info("create node request: {}", createNodeRequest);
        CreateNodeResponse response;
        try {
            response = client.createNode(createNodeRequest);
        } catch (Exception e) {
            log.error("create node failed", e);
            throw new RuntimeException(e);
        }
        if (response == null) {
            throw new RuntimeException("response is null");
        }
        if (response.getStatusCode() != null && SUCCESS_STATUS != response.getStatusCode()) {
            log.error("create node failed, requestId: {}", response.getBody().getRequestId());
            throw new RuntimeException("create node failed");
        }
        log.info("create node success, response: {}", JSONUtils.toJsonString(response));
        return Optional.of(response)
                .map(CreateNodeResponse::getBody)
                .map(CreateNodeResponseBody::getId)
                .orElse(null);
    }

    /**
     * update node
     *
     * @param updateNodeRequest update node request
     * @return is or not success
     */
    public boolean updateNode(UpdateNodeRequest updateNodeRequest) {
        checkInit();
        log.info("update node request: {}", updateNodeRequest);
        UpdateNodeResponse response;
        try {
            response = client.updateNode(updateNodeRequest);
        } catch (Exception e) {
            log.error("update node failed", e);
            throw new RuntimeException(e);
        }
        if (response == null) {
            throw new RuntimeException("response is null");
        }
        if (response.getStatusCode() != null && SUCCESS_STATUS != response.getStatusCode()) {
            log.error("update node failed, requestId: {}", response.getBody().getRequestId());
            return false;
        }
        log.info("update node success, response: {}", JSONUtils.toJsonString(response));
        return Optional.of(response)
                .map(UpdateNodeResponse::getBody)
                .map(UpdateNodeResponseBody::getSuccess)
                .orElse(false);
    }

    /**
     * create workflow
     *
     * @param request create workflow request
     * @return workflow id
     */
    public String createWorkflow(CreateWorkflowDefinitionRequest request) {
        checkInit();
        log.info("create workflow request: {}", request);
        CreateWorkflowDefinitionResponse response;
        try {
            response = client.createWorkflowDefinition(request);
        } catch (Exception e) {
            log.error("create workflow failed", e);
            throw new RuntimeException(e);
        }
        if (response == null) {
            throw new RuntimeException("response is null");
        }
        if (response.getStatusCode() != null && SUCCESS_STATUS != response.getStatusCode()) {
            log.error("create workflow failed, requestId: {}", response.getBody().getRequestId());
            throw new RuntimeException("create workflow failed");
        }
        log.info("create workflow response: {}", JSONUtils.toJsonString(response));
        return Optional.of(response)
                .map(CreateWorkflowDefinitionResponse::getBody)
                .map(CreateWorkflowDefinitionResponseBody::getId)
                .orElse(null);
    }

    /**
     * update workflow
     *
     * @param request update workflow request
     * @return is or not success
     */
    public boolean updateWorkflow(UpdateWorkflowDefinitionRequest request) {
        checkInit();
        log.info("update workflow request: {}", request);
        UpdateWorkflowDefinitionResponse response;
        try {
            response = client.updateWorkflowDefinition(request);
        } catch (Exception e) {
            log.error("update workflow failed", e);
            throw new RuntimeException(e);
        }
        if (response == null) {
            throw new RuntimeException("response is null");
        }
        if (response.getStatusCode() != null && SUCCESS_STATUS != response.getStatusCode()) {
            log.error("update workflow failed, requestId: {}", response.getBody().getRequestId());
            return false;
        }
        log.info("update workflow response: {}", JSONUtils.toJsonString(response));
        return Optional.of(response)
                .map(UpdateWorkflowDefinitionResponse::getBody)
                .map(UpdateWorkflowDefinitionResponseBody::getSuccess)
                .orElse(false);
    }

    private boolean nodeExist(String projectId, String uuid) {
        return getNode(projectId, uuid) != null;
    }

    private boolean workflowExist(String projectId, String uuid) {
        return getWorkflow(projectId, uuid) != null;
    }

    /**
     * get node info
     *
     * @param projectId project id
     * @param uuid      uuid
     * @return node info
     */
    public GetNodeResponseBodyNode getNode(String projectId, String uuid) {
        if (StringUtils.isBlank(uuid)) {
            return null;
        }
        checkInit();
        GetNodeRequest request = new GetNodeRequest()
                .setProjectId(projectId)
                .setId(uuid);
        log.info("GetNodeRequest: {}", request);
        GetNodeResponse response;
        try {
            response = client.getNode(request);
        } catch (Exception e) {
            log.warn("get node failed", e);
            return null;
        }
        log.info("GetNodeResponse: {}", JSONUtils.toJsonString(response));
        return Optional.ofNullable(response)
                .map(GetNodeResponse::getBody)
                .map(GetNodeResponseBody::getNode)
                .orElse(null);
    }

    /**
     * get workflow info
     *
     * @param projectId project id
     * @param uuid      uuid
     * @return workflow info
     */
    public GetWorkflowDefinitionResponseBodyWorkflowDefinition getWorkflow(String projectId, String uuid) {
        if (StringUtils.isBlank(uuid)) {
            return null;
        }
        checkInit();
        GetWorkflowDefinitionRequest request = new GetWorkflowDefinitionRequest()
                .setProjectId(projectId)
                .setId(uuid);
        log.info("GetWorkflowDefinitionRequest: {}", request);
        GetWorkflowDefinitionResponse response;
        try {
            response = client.getWorkflowDefinition(request);
        } catch (Exception e) {
            log.warn("get workflow failed", e);
            return null;
        }
        log.info("GetWorkflowDefinitionResponse: {}", JSONUtils.toJsonString(response));
        return Optional.ofNullable(response)
                .map(GetWorkflowDefinitionResponse::getBody)
                .map(GetWorkflowDefinitionResponseBody::getWorkflowDefinition)
                .orElse(null);
    }

    public ListWorkflowDefinitionsResponseBodyPagingInfo listWorkflows(String projectId, Integer pageNumber, Integer pageSize) {
        checkInit();
        ListWorkflowDefinitionsRequest request = new ListWorkflowDefinitionsRequest()
                .setProjectId(projectId)
                .setPageNumber(pageNumber)
                .setPageSize(pageSize);
        log.info("ListWorkflowDefinitionsRequest: {}", request);
        ListWorkflowDefinitionsResponse response;
        try {
            response = client.listWorkflowDefinitions(request);
        } catch (Exception e) {
            log.error("list workflow failed", e);
            return null;
        }
        log.info("ListWorkflowDefinitionsResponse: {}", JSONUtils.toJsonString(response));
        return Optional.ofNullable(response)
                .map(ListWorkflowDefinitionsResponse::getBody)
                .map(ListWorkflowDefinitionsResponseBody::getPagingInfo)
                .orElse(null);
    }

    /**
     * list node dependencies
     *
     * @param projectId project id
     * @param uuid      uuid
     * @return node dependencies page info
     */
    public ListNodeDependenciesResponseBodyPagingInfo listNodeDependencies(String projectId, String uuid) {
        if (StringUtils.isBlank(uuid)) {
            return null;
        }
        checkInit();
        ListNodeDependenciesRequest request = new ListNodeDependenciesRequest()
                .setProjectId(projectId)
                .setId(uuid);
        log.info("ListNodeDependenciesRequest: {}", request);
        ListNodeDependenciesResponse response;
        try {
            response = client.listNodeDependencies(request);
        } catch (Exception e) {
            log.error("list node dependencies failed", e);
            return null;
        }
        log.info("ListNodeDependenciesResponse: {}", JSONUtils.toJsonString(response));
        return Optional.ofNullable(response)
                .map(ListNodeDependenciesResponse::getBody)
                .map(ListNodeDependenciesResponseBody::getPagingInfo)
                .orElse(null);
    }
}
