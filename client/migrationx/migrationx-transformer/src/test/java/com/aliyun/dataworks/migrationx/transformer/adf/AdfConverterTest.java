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
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class AdfConverterTest {
    @Test
    public void testAdfConf() throws IOException {
        AdfConf adfConf = prepareAdfConf("src/test/resources/json/adf/conf.json");
        List<AdfConf.AdfSetting.Variable> vars = adfConf.getSettings().getGlobalVariables();
        Assert.assertEquals(2, vars.size());
        AdfConf.AdfSetting.Variable var = vars.get(0);
        Assert.assertEquals("time", var.name);
        Assert.assertEquals("${workspace.time}", var.value);
        var = vars.get(1);
        Assert.assertEquals("env", var.name);
        Assert.assertEquals("${workspace.env}", var.value);
    }

    @Test
    public void testNotebookPathConversionWithHappyPath() {
        String originalPath = "/Repos/NGBI/PRD/DM/dm_td_calendar";
        Map<String, String> map = ImmutableMap.of("removePrefix", "/Repos/NGBI/", "addPrefix", "/Users/abc/Documents/github_repos/");
        String localPath = AdfConverter.getLocalPath(map, originalPath);
        Assert.assertEquals("/Users/abc/Documents/github_repos/PRD/DM/dm_td_calendar", localPath);
    }

    @Test
    public void testNotebookPathConversionWithInvalidInput() {
        Map<String, String> map = ImmutableMap.of("removePrefix", "/Repos/NGBI/", "addPrefix", "/Users/abc/Documents/github_repos/");
        Assert.assertEquals("", AdfConverter.getLocalPath(map, ""));
        Assert.assertNull(AdfConverter.getLocalPath(map, null));
    }

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
        Assert.assertEquals(1, AdfConverter.toTimeoutInHours("0.1:00:00"));
    }

    @Test
    public void testGetValidName() {
        Assert.assertEquals("Refresh_DDI_After_RetailBI_Ready", AdfConverter.getValidName("Refresh-DDI    After RetailBI Ready"));
        Assert.assertEquals("Refresh_DDI_After_RetailBI_Ready", AdfConverter.getValidName("Refresh    DDI    After RetailBI Ready"));
        Assert.assertEquals("Refresh_DDI_After_RetailBI_Ready", AdfConverter.getValidName("Refresh DDI After RetailBI Ready"));
    }

    private AdfConf prepareAdfConf(String path) throws IOException {
        String fileContent = FileUtils.readFileToString(new File(path), StandardCharsets.UTF_8);
        return JSONUtils.parseObject(fileContent, AdfConf.class);
    }
}
