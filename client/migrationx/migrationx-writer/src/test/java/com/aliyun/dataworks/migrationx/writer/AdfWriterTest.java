package com.aliyun.dataworks.migrationx.writer;

import com.aliyun.dataworks.migrationx.writer.dataworks.DataWorksFlowSpecWriter;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class AdfWriterTest {

    @Test
    public void testWriter() {
        DataWorksFlowSpecWriter writer = new DataWorksFlowSpecWriter();
        String region = "cn-hangzhou";
        String accessKey = "";
        String accessSecret = "";
        String filePath = "/Users/xichen/Documents/repos/alibabacloud-dataworks-tool-migration/client/migrationx/migrationx-transformer/src/test/resources/json/adf/workflow1.json";
        writer.doSubmitFile(region, "528891", accessKey, accessSecret, filePath);
    }

    @Test
    public void testNestedWorkflowWriter() {
        DataWorksFlowSpecWriter writer = new DataWorksFlowSpecWriter();
        String region = "cn-shanghai";
        String projectId_shanghai = "528891";
        String accessKey = "";
        String accessSecret = "";
        String filePath = "/Users/xichen/Documents/repos/alibabacloud-dataworks-tool-migration/client/temp/package/.tmp/main_flow.json";
        writer.doSubmitFile(region, projectId_shanghai, accessKey, accessSecret, filePath);

    }
    @Test
    public void testSubmitToDataWorks() throws Exception {
        DataWorksFlowSpecWriter writer = new DataWorksFlowSpecWriter();
        String region = "cn-hangzhou";
        String projectId = "56629";
        String akId = "";
        String akSecret = "";
        String file = "/Users/xichen/Documents/repos/alibabacloud-dataworks-tool-migration/client/temp/package/.tmp/pipeline_department1_1.json";
        String[] args = new String[]{
                "-r", region,
                "-p", projectId,
                "-f", file,
                "-i", akId,
                "-s", akSecret
        };
        writer.run(args);
    }
}
