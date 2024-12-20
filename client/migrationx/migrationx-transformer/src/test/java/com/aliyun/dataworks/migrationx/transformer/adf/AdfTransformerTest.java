package com.aliyun.dataworks.migrationx.transformer.adf;

import com.aliyun.dataworks.migrationx.transformer.dataworks.apps.DataWorksAdfTransformerApp;
import org.junit.Ignore;
import org.junit.Test;
@Ignore
public class AdfTransformerTest {
    @Test
    public void testTransformer() {
        DataWorksAdfTransformerApp transformerApp = new DataWorksAdfTransformerApp();
        String[] args = new String[]{
                "-c", "src/main/conf/adf-mc-transformer-config.json",
                "-s", "/Users/xichen/Documents/idea_projects/json/",
                "-t", "/Users/xichen/Documents/idea_projects/target/"
        };
        transformerApp.run(args);
    }
}
