package com.aliyun.dataworks.migrationx.writer;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class DataWorksMigrationAssistWriterTest {

    @Test
    public void test1() throws Exception {
        DataWorksMigrationAssistWriter writer = new DataWorksMigrationAssistWriter();
        String baseDir = "/Users/xx/Documents/workspace/alibabacloud-dataworks-tool-migration";
        String source = String.format("%s/temp/target3.zip", baseDir);
        String[] args = new String[]{
                //"-e", "dataworks.cn-shenzhen.aliyuncs.com",  //endpoint
                "-i", "xx",  //accessId
                "-k", "xxx",  //accessKey
                "-r", "cn-shanghai",   //regionId
                "-p", "483776",   //projectId
                "-f", source,    //file
                "-t", "SPEC",   //spect
        };
        writer.run(args);
    }
}