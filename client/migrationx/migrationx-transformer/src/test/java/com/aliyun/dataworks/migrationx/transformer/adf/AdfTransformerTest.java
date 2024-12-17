package com.aliyun.dataworks.migrationx.transformer.adf;

import com.aliyun.dataworks.migrationx.transformer.dataworks.apps.DataWorksAdfTransformerApp;
import org.junit.Ignore;
import org.junit.Test;
@Ignore
public class AdfTransformerTest {

    @Test
    public void test1() {
        DataWorksAdfTransformerApp transformerApp = new DataWorksAdfTransformerApp();
        String[] args = new String[]{
                "-c", "/Users/xichen/Documents/repos/alibabacloud-dataworks-tool-migration/client/migrationx/migrationx-transformer/src/main/conf/adf-mc-transformer-config.json",
                "-s", "/Users/xichen/Documents/repos/alibabacloud-dataworks-tool-migration/client/temp/adf/.tmp/chenxi-df/",
                "-t", "/Users/xichen/Documents/repos/alibabacloud-dataworks-tool-migration/client/temp/package/"
        };
        transformerApp.run(args);
    }
}
