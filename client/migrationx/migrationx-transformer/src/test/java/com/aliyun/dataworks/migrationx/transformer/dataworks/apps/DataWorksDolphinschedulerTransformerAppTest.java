package com.aliyun.dataworks.migrationx.transformer.dataworks.apps;

import java.io.File;

import com.aliyun.dataworks.migrationx.transformer.core.BaseTransformerApp;

import lombok.extern.slf4j.Slf4j;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
@Slf4j
public class DataWorksDolphinschedulerTransformerAppTest {

    @Test
    public void test1() {
        BaseTransformerApp transformerApp = new DataWorksDolphinschedulerTransformerApp();
        File fil = new File(".");
        log.info("{}", fil.getAbsolutePath());
        String[] args = new String[]{
                "-c", "../../../temp/conf/transformer.json",
                //"-s", "../../temp/13666515015680/.tmp",
                "-s", "../../../temp/test1111",
                //"-s", "../../temp/datax",
                "-t", "../../../temp/target3.zip"
        };
        transformerApp.run(args);
    }
}