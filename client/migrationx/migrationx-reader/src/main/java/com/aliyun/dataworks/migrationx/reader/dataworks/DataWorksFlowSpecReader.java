package com.aliyun.dataworks.migrationx.reader.dataworks;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.aliyun.dataworks.client.command.CommandApp;
import com.aliyun.dataworks.migrationx.domain.dataworks.service.openapi.DataWorksOpenApiService;
import com.aliyun.dataworks_public20240518.models.GetWorkflowDefinitionResponseBody.GetWorkflowDefinitionResponseBodyWorkflowDefinition;
import com.aliyun.dataworks_public20240518.models.ListWorkflowDefinitionsResponseBody.ListWorkflowDefinitionsResponseBodyPagingInfo;
import com.aliyun.dataworks_public20240518.models.ListWorkflowDefinitionsResponseBody.ListWorkflowDefinitionsResponseBodyPagingInfoWorkflowDefinitions;
import com.aliyun.migrationx.common.exception.BizException;
import com.aliyun.migrationx.common.exception.ErrorCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-10-10
 */
@Slf4j
public class DataWorksFlowSpecReader extends CommandApp {

    public static void main(String[] args) throws Exception {
        new DataWorksFlowSpecReader().run(args);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addRequiredOption("e", "endpoint", true, "dataworks endpoint, example: dataworks.cn-hangzhou.aliyuncs.com");
        options.addRequiredOption("p", "projectId", true, "dataworks project id");
        options.addRequiredOption("f", "file", true, "flow spec json file");
        // dataworks auth, using env variable is supported.
        options.addOption("ak", "accessKeyId", true, "dataworks access key id");
        options.addOption("sk", "accessKeySecret", true, "dataworks access key secret");
        // filter by workflow id
        Option workflowIdOption = Option.builder("w")
            .longOpt("workflowId")
            .numberOfArgs(Option.UNLIMITED_VALUES)
            .desc("workflow id, multiple ids separated by space").build();
        options.addOption(workflowIdOption);
        return options;
    }

    @Override
    protected void doCommandRun(Options options, CommandLine cli, String[] args) {
        String endpoint = cli.getOptionValue("e");
        String projectId = cli.getOptionValue("p");
        String filePath = cli.getOptionValue("f");
        String[] workflowIds = cli.getOptionValues("w");
        String accessKeyId = cli.getOptionValue("ak");
        String accessKeySecret = cli.getOptionValue("sk");

        DataWorksOpenApiService dataWorksOpenApiService = new DataWorksOpenApiService(endpoint, projectId, accessKeyId, accessKeySecret);
        File file = new File(filePath);
        if (file.exists() && !file.delete()) {
            log.error("target file exists and can not be deleted, file: {}", filePath);
            throw new BizException(ErrorCode.NO_PERMISSION, "delete file " + filePath);
        }
        try {
            BufferedWriter writer = new BufferedWriter(new java.io.FileWriter(file));
            readFlowSpec(dataWorksOpenApiService, projectId, new HashSet<>(Arrays.asList(ObjectUtils.defaultIfNull(workflowIds, new String[] {}))),
                writer);
        } catch (IOException e) {
            log.error("write to target file error", e);
        }
        log.info("read dataworks workflow success, flow spec json target path: {}", file);
    }

    public void readFlowSpec(DataWorksOpenApiService dataWorksOpenApiService, String projectId,
                             Set<String> workflowIds, BufferedWriter writer) {
        int pageNo = 0;
        int pageSize = 10;
        int total = 0;
        boolean isFirst = true;
        boolean conditionEmpty = CollectionUtils.isEmpty(workflowIds);
        do {
            pageNo++;
            log.info("read dataworks workflow, page: {}, total: {}", pageNo, total);
            try {
                ListWorkflowDefinitionsResponseBodyPagingInfo listWorkflowDefinitionsResponseBodyPagingInfo = dataWorksOpenApiService.listWorkflows(
                    projectId, pageNo, pageSize);
                if (listWorkflowDefinitionsResponseBodyPagingInfo == null) {
                    log.warn("error occur when list workflow, response is null, projectId: {}, pageNum: {}, pageSize: {}",
                        projectId, pageNo, pageSize);
                    continue;
                }
                total = listWorkflowDefinitionsResponseBodyPagingInfo.getTotalCount();
                List<String> workflowIdList = ListUtils.emptyIfNull(listWorkflowDefinitionsResponseBodyPagingInfo.getWorkflowDefinitions()).stream()
                    .map(ListWorkflowDefinitionsResponseBodyPagingInfoWorkflowDefinitions::getId)
                    .filter(id -> conditionEmpty || workflowIds.contains(id))
                    .collect(Collectors.toList());
                for (String workflowId : workflowIdList) {
                    // reduce the impact of exceptions to ensure that other workflows can be written
                    try {
                        GetWorkflowDefinitionResponseBodyWorkflowDefinition workflow = dataWorksOpenApiService.getWorkflow(projectId, workflowId);
                        String spec = Optional.ofNullable(workflow)
                            .map(GetWorkflowDefinitionResponseBodyWorkflowDefinition::getSpec).orElse(null);
                        if (StringUtils.isNotBlank(spec)) {
                            if (isFirst) {
                                writer.write("[");
                                isFirst = false;
                            } else {
                                writer.write(",");
                                writer.newLine();
                            }
                            writer.newLine();
                            writer.write(spec);
                        }
                    } catch (Exception e) {
                        log.error("read single workflow data error, workflow id: {}", workflowId, e);
                    }
                }
                writer.flush();
            } catch (Exception e) {
                log.error("read dataworks workflow data error, pageNum: {}, pageSize: {}", pageNo, pageSize, e);
            }
        } while (pageNo * pageSize < total);
        // close json array
        try {
            writer.newLine();
            writer.write("]");
            writer.flush();
        } catch (IOException e) {
            log.error("write json array end token error", e);
        }
    }
}