package com.aliyun.dataworks.migrationx.transformer.flowspec.app;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class DolphinSchedulerV3FlowSpecTransformerAppTest {

    @Test
    public void testTransSpec() throws Exception {
        String baseDir = "/Users/lunjianchang/Documents/workspace/alibabacloud-dataworks-tool-migration";
        DolphinSchedulerV3FlowSpecTransformerApp app = new DolphinSchedulerV3FlowSpecTransformerApp();
        String[] args = new String[]{
                "-c", String.format("%s/temp/conf/transformer.json", baseDir),
                //"-s", "../../temp/13666515015680/.tmp",
                "-s", String.format("%s/temp/15544980547136/.tmp/", baseDir),
                //"-s", "../../temp/datax",
                "-t", String.format("%s/temp/spec/target4", baseDir)
        };
        app.run(args);
    }
}