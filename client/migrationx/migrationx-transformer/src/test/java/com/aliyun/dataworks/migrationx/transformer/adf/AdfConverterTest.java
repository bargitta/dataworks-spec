package com.aliyun.dataworks.migrationx.transformer.adf;

import com.aliyun.dataworks.common.spec.SpecUtil;
import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.enums.SpecVersion;
import com.aliyun.dataworks.common.spec.domain.interfaces.Output;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.adf.*;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.adf.AdfConverter;
import com.aliyun.migrationx.common.utils.JSONUtils;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class AdfConverterTest {
    @Test
    public void testOutput() {
        AdfPackage adfPackage = new AdfPackage();
        AdfConverter converter = new AdfConverter(adfPackage, new AdfConf());
        Pipeline pipeline = new Pipeline();
        pipeline.setName("databricks");
        List<Output> outputs = converter.getOutput("id", pipeline.getName());
        SpecNodeOutput output = (SpecNodeOutput) outputs.get(0);
        Assert.assertEquals("databricks", output.getRefTableName());
        Assert.assertEquals("id", output.getData());
    }

    @Test
    public void testPipeline() throws Exception {
        String folder = "src/test/resources/json/adf";
        AdfPackageLoader loader = new AdfPackageLoader(new File(folder));
        AdfPackage adfPackage = loader.loadPackage();
        AdfConverter converter = new AdfConverter(adfPackage, prepareAdfConf("src/test/resources/json/adf/conf.json"));
        List<SpecWorkflow> workflows = converter.convert();
        DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
        spec.setWorkflows(workflows);
        Specification<DataWorksWorkflowSpec> sp = new Specification<>();
        sp.setKind(SpecKind.CYCLE_WORKFLOW.getLabel());
        sp.setVersion(SpecVersion.V_1_1_0.getLabel());
        sp.setSpec(spec);
        FileUtils.writeStringToFile(new File("src/test/resources/json/adf/workflow1.json"), SpecUtil.writeToSpec(sp), StandardCharsets.UTF_8);
    }

    @Test
    public void testTimeout() {
        Assert.assertEquals(12, AdfConverter.toTimeoutInHours("0.12:00:00"));
        Assert.assertEquals(12, AdfConverter.toTimeoutInHours("0.12:33:00"));
        Assert.assertEquals(60, AdfConverter.toTimeoutInHours("2.12:33:00"));


    }

    private AdfConf prepareAdfConf(String path) throws IOException {
        String fileContent = FileUtils.readFileToString(new File(path), StandardCharsets.UTF_8);
        return JSONUtils.parseObject(fileContent, AdfConf.class);
    }
}
