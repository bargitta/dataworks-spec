package com.aliyun.dataworks.migrationx.domain.dataworks.service.spec;

import java.util.Collections;
import java.util.Date;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.FunctionType;
import com.aliyun.dataworks.common.spec.domain.enums.NodeInstanceModeType;
import com.aliyun.dataworks.common.spec.domain.enums.NodeRerunModeType;
import com.aliyun.dataworks.common.spec.domain.enums.SpecFileResourceType;
import com.aliyun.dataworks.common.spec.domain.enums.TriggerType;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDepend;
import com.aliyun.dataworks.common.spec.domain.ref.SpecDatasource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecFileResource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecFunction;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecRuntimeResource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecTrigger;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client.File;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client.FileNodeCfg;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client.FileNodeInputOutput;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client.FileNodeInputOutputContext;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client.NodeType;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.types.DependentType;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.types.NodeUseType;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.types.RerunMode;
import com.aliyun.dataworks.migrationx.domain.dataworks.service.spec.entity.FileDetailEntityAdapter;
import com.aliyun.dataworks.migrationx.domain.dataworks.service.spec.entity.SpecFileResourceEntityAdapter;
import com.aliyun.dataworks.migrationx.domain.dataworks.service.spec.entity.SpecFunctionEntityAdapter;
import com.aliyun.dataworks.migrationx.domain.dataworks.service.spec.entity.SpecNodeEntityAdapter;
import com.aliyun.migrationx.common.utils.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-11-22
 */
@Slf4j
public class NodeSpecUpdateAdapterTest {

    @Test
    public void testSimpleCreate() {
        File file = new File();
        FileNodeCfg fileNodeCfg = new FileNodeCfg();
        fillFile(file);
        fillNodeCfg(fileNodeCfg);

        FileDetailEntityAdapter adapter = new FileDetailEntityAdapter();
        adapter.setFile(file);
        adapter.setFileNodeCfg(fileNodeCfg);
        Specification<DataWorksWorkflowSpec> specification = new Specification<>();
        new NodeSpecUpdateAdapter().updateSpecification(adapter, specification);

        DataWorksWorkflowSpec spec = specification.getSpec();
        SpecNode specNode = spec.getNodes().stream().findFirst().orElse(null);
        Assert.assertNotNull(specNode);
        Assert.assertEquals(String.valueOf(file.getFileId()), specNode.getId());
        Assert.assertEquals(file.getFileName(), specNode.getName());
        Assert.assertEquals(file.getFileDesc(), specNode.getDescription());
        Assert.assertEquals("test/path/test_file_name", specNode.getScript().getPath());
        Assert.assertEquals(CodeProgramType.ODPS_SQL.name(), specNode.getScript().getRuntime().getCommand());
        Assert.assertEquals(file.getOwner(), specNode.getOwner());
        Assert.assertEquals(file.getContent(), specNode.getScript().getContent());

        Assert.assertEquals(fileNodeCfg.getTaskRerunTime(), specNode.getRerunTimes());
        Assert.assertEquals(fileNodeCfg.getTaskRerunInterval(), specNode.getRerunInterval());
        Assert.assertEquals(NodeRerunModeType.ALL_ALLOWED, specNode.getRerunMode());
        Assert.assertEquals(3, specNode.getScript().getParameters().size());
        Assert.assertEquals("aaa", specNode.getScript().getParameters().get(0).getName());
        Assert.assertEquals("1", specNode.getScript().getParameters().get(0).getValue());
        Assert.assertEquals("bbb", specNode.getScript().getParameters().get(1).getName());
        Assert.assertEquals("2", specNode.getScript().getParameters().get(1).getValue());
        Assert.assertEquals(fileNodeCfg.getCronExpress(), specNode.getTrigger().getCron());
        Assert.assertNotNull(specNode.getTrigger().getStartTime());
        Assert.assertNotNull(specNode.getTrigger().getEndTime());
        Assert.assertEquals(NodeInstanceModeType.IMMEDIATELY, specNode.getInstanceMode());
        Assert.assertEquals(false, specNode.getAutoParse());
        Assert.assertNull(specNode.getIgnoreBranchConditionSkip());
        Assert.assertEquals("ccc", specNode.getScript().getParameters().get(2).getName());
        Assert.assertNull(specNode.getScript().getParameters().get(2).getValue());

        Assert.assertEquals(2, specNode.getInputs().size());
    }

    @Test
    public void testSimpleUpdate() {
        File file = new File();
        FileNodeCfg fileNodeCfg = new FileNodeCfg();
        fillFile(file);
        fillNodeCfg(fileNodeCfg);

        FileDetailEntityAdapter adapter = new FileDetailEntityAdapter();
        adapter.setFile(file);
        adapter.setFileNodeCfg(fileNodeCfg);
        Specification<DataWorksWorkflowSpec> specification = new Specification<>();
        new NodeSpecUpdateAdapter().updateSpecification(adapter, specification);

        file.setFileDesc("test_description_update");
        new NodeSpecUpdateAdapter().updateSpecification(adapter, specification);
        SpecNode specNode = specification.getSpec().getNodes().stream().findFirst().orElse(null);
        Assert.assertNotNull(specNode);
        Assert.assertEquals(file.getFileDesc(), specNode.getDescription());
    }

    @Test
    public void testFileDetailEntityAdapter() {
        File file = new File();
        FileNodeCfg fileNodeCfg = new FileNodeCfg();
        fillFile(file);
        fillNodeCfg(fileNodeCfg);

        FileDetailEntityAdapter adapter = new FileDetailEntityAdapter();
        adapter.setFile(file);
        adapter.setFileNodeCfg(fileNodeCfg);

        Assert.assertEquals(String.valueOf(file.getFileId()), adapter.getUuid());
        Assert.assertEquals(file.getFileFolderPath(), adapter.getFolder());
        Assert.assertEquals(file.getFileName(), adapter.getName());
        Assert.assertEquals(file.getFileType(), adapter.getTypeId());
        Assert.assertEquals(file.getOwner(), adapter.getOwner());
        Assert.assertEquals(file.getContent(), adapter.getCode());
        Assert.assertEquals(file.getFileDesc(), adapter.getDescription());

        Assert.assertEquals(fileNodeCfg.getTaskRerunTime(), adapter.getTaskRerunTime());
        Assert.assertEquals(fileNodeCfg.getTaskRerunInterval(), adapter.getTaskRerunInterval());
        Assert.assertEquals(fileNodeCfg.getStartEffectDate(), adapter.getStartEffectDate());
        Assert.assertEquals(fileNodeCfg.getEndEffectDate(), adapter.getEndEffectDate());
        Assert.assertEquals(fileNodeCfg.getCronExpress(), adapter.getCronExpress());
        Assert.assertEquals(fileNodeCfg.getCycleType(), adapter.getCycleType());
        Assert.assertEquals(fileNodeCfg.getIsAutoParse(), adapter.getIsAutoParse());
        Assert.assertEquals(fileNodeCfg.getNodeType(), adapter.getNodeType());
        Assert.assertEquals(fileNodeCfg.getStartRightNow(), adapter.getStartRightNow());
        Assert.assertEquals(fileNodeCfg.getParaValue(), adapter.getParameter());
        Assert.assertEquals(adapter.getInputContexts().size(), 1);
        Assert.assertEquals(adapter.getNodeType(), (Integer)NodeType.NORMAL.getCode());

        Assert.assertNull(adapter.getBizId());
        Assert.assertNull(adapter.getBizName());
        Assert.assertNotNull(adapter.getStartEffectDate());
        Assert.assertNotNull(adapter.getEndEffectDate());
        Assert.assertNull(adapter.getDiResourceGroup());
        Assert.assertNull(adapter.getDiResourceGroupName());
        Assert.assertNull(adapter.getCodeMode());
        Assert.assertEquals(adapter.getRerunMode(), RerunMode.ALL_ALLOWED);
        Assert.assertNull(adapter.getPauseSchedule());
        Assert.assertEquals(adapter.getNodeUseType(), NodeUseType.SCHEDULED);
        Assert.assertNull(adapter.getRef());
        Assert.assertNull(adapter.getRoot());
        Assert.assertNull(adapter.getOutputContexts());
        Assert.assertEquals(adapter.getInnerNodes(), Collections.emptyList());
        Assert.assertNull(adapter.getDependentType());
        Assert.assertNull(adapter.getLastModifyTime());
        Assert.assertNull(adapter.getLastModifyUser());
        Assert.assertNull(adapter.getMultiInstCheckType());
        Assert.assertNull(adapter.getPriority());
        Assert.assertNull(adapter.getDependentDataNode());
        Assert.assertNull(adapter.getOwnerName());
        Assert.assertNull(adapter.getExtraConfig());
        Assert.assertNull(adapter.getExtraContent());
        Assert.assertNull(adapter.getTtContent());
        Assert.assertNull(adapter.getAdvanceSettings());
        Assert.assertNull(adapter.getExtend());
        Assert.assertNull(adapter.getComponent());
        Assert.assertNull(adapter.getImageId());
        Assert.assertNull(adapter.getCalendarId());
        Assert.assertNull(adapter.getStreamLaunchMode());
        Assert.assertNull(adapter.getIgnoreBranchConditionSkip());
        Assert.assertNull(adapter.getParentId());
        Assert.assertNull(adapter.getCu());
        Assert.assertNull(adapter.getOrigin());
        Assert.assertNull(adapter.getWorkflowName());
        Assert.assertNull(adapter.getConfigPack());

    }

    private void fillFile(File file) {
        file.setFileId(123456789L);
        file.setFileFolderPath("test/path");
        file.setFileName("test_file_name");
        file.setFileType(10);
        file.setOwner("111222");
        file.setContent("select 1;");
        file.setUseType(NodeUseType.SCHEDULED.getValue());
        file.setFileDesc("test_description");
    }

    private void fillNodeCfg(FileNodeCfg fileNodeCfg) {
        fileNodeCfg.setTaskRerunTime(10);
        fileNodeCfg.setTaskRerunInterval(60000);
        fileNodeCfg.setReRunAble(1);
        fileNodeCfg.setParaValue("aaa=1 bbb=2");
        fileNodeCfg.setStartEffectDate(new Date());
        fileNodeCfg.setEndEffectDate(new Date());
        fileNodeCfg.setCronExpress("00 00 03 * * ?");
        fileNodeCfg.setCycleType(1);
        fileNodeCfg.setIsAutoParse(0);
        FileNodeInputOutput o = new FileNodeInputOutput();
        o.setStr("7394842754146461382");
        fileNodeCfg.setInputList(Collections.singletonList(o));
        fileNodeCfg.setNodeType(0);
        FileNodeInputOutputContext ctxInput = new FileNodeInputOutputContext();
        ctxInput.setParamName("ccc");
        ctxInput.setParamValue("7394842754146461382:zzz");
        fileNodeCfg.setInputContextList(Collections.singletonList(ctxInput));
        fileNodeCfg.setStartRightNow(true);
    }

    @Test
    public void testSpecNodeEntityAdapter() {
        SpecNodeEntityAdapter specNodeEntityAdapter = new SpecNodeEntityAdapter();
        SpecNode specNode = new SpecNode();
        specNode.setId("123456789");
        specNode.setName("test");
        specNode.setDescription("description");
        specNode.setOwner("446209");

        SpecScript script = new SpecScript();
        script.setPath("path/test");
        script.setContent("content");
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setCommand(CodeProgramType.ODPS_SQL.getName());
        script.setRuntime(runtime);
        specNode.setScript(script);

        SpecTrigger specTrigger = new SpecTrigger();
        specTrigger.setStartTime("1970-01-01 00:00:00");
        specTrigger.setEndTime("2025-01-01 00:00:00");
        specTrigger.setType(TriggerType.SCHEDULER);
        specTrigger.setCron("0 0 0 * * ?");
        specTrigger.setTimezone("Asia/Shanghai");
        specNode.setTrigger(specTrigger);

        SpecRuntimeResource specRuntimeResource = new SpecRuntimeResource();
        specRuntimeResource.setResourceGroup("resourceGroup");
        SpecDatasource specDatasource = new SpecDatasource();
        specDatasource.setId("111");
        specDatasource.setName("conn");
        specNode.setRuntimeResource(specRuntimeResource);
        specNode.setDatasource(specDatasource);

        SpecNodeOutput specNodeOutput = new SpecNodeOutput();
        specNodeOutput.setData("123456789");
        specNodeOutput.setIsDefault(true);
        specNodeOutput.setRefTableName("test");
        specNode.setOutputs(Collections.singletonList(specNodeOutput));

        SpecDepend specDepend = new SpecDepend();
        SpecNodeOutput input = new SpecNodeOutput();
        input.setData("987");
        specDepend.setOutput(input);

        specNodeEntityAdapter.setNode(specNode);
        specNodeEntityAdapter.setSpecDepends(Collections.singletonList(specDepend));

        Assert.assertEquals(specNodeEntityAdapter.getUuid(), specNode.getId());
        Assert.assertEquals(specNodeEntityAdapter.getName(), specNode.getName());
        Assert.assertEquals(specNodeEntityAdapter.getDescription(), specNode.getDescription());
        Assert.assertEquals(specNodeEntityAdapter.getOwner(), specNode.getOwner());
        Assert.assertEquals(specNodeEntityAdapter.getFolder(), "path");
        Assert.assertEquals(specNodeEntityAdapter.getCode(), script.getContent());
        Assert.assertEquals(specNodeEntityAdapter.getType(), runtime.getCommand());
        Assert.assertEquals(specNodeEntityAdapter.getResourceGroup(), specRuntimeResource.getResourceGroup());
        Assert.assertEquals(specNodeEntityAdapter.getResourceGroupName(), specRuntimeResource.getResourceGroup());

        Assert.assertEquals(specNodeEntityAdapter.getConnection(), specDatasource.getName());
        Assert.assertEquals(specNodeEntityAdapter.getOutputs().get(0).getData(), specNodeOutput.getData());
        Assert.assertEquals(specNodeEntityAdapter.getInputs().get(0).getData(), input.getData());

        Assert.assertEquals(specNodeEntityAdapter.getCronExpress(), "0 0 0 * * ?");
        Assert.assertEquals(specNodeEntityAdapter.getStartEffectDate(), DateUtils.convertStringToDate("1970-01-01 00:00:00"));
        Assert.assertEquals(specNodeEntityAdapter.getEndEffectDate(), DateUtils.convertStringToDate("2025-01-01 00:00:00"));

        Assert.assertNull(specNodeEntityAdapter.getBizId());
        Assert.assertNull(specNodeEntityAdapter.getBizName());
        Assert.assertEquals(specNodeEntityAdapter.getTypeId(), (Integer)CodeProgramType.ODPS_SQL.getCode());
        Assert.assertNull(specNodeEntityAdapter.getDiResourceGroup());
        Assert.assertNull(specNodeEntityAdapter.getDiResourceGroupName());
        Assert.assertNull(specNodeEntityAdapter.getCodeMode());
        Assert.assertEquals(specNodeEntityAdapter.getStartRightNow(), false);
        Assert.assertEquals(specNodeEntityAdapter.getRerunMode(), RerunMode.UNKNOWN);
        Assert.assertNull(specNodeEntityAdapter.getNodeType());
        Assert.assertEquals(specNodeEntityAdapter.getPauseSchedule(), false);
        Assert.assertEquals(specNodeEntityAdapter.getIsAutoParse(), Integer.valueOf(0));
        Assert.assertEquals(specNodeEntityAdapter.getNodeUseType(), NodeUseType.SCHEDULED);
        Assert.assertNull(specNodeEntityAdapter.getRef());
        Assert.assertNull(specNodeEntityAdapter.getRoot());
        Assert.assertNull(specNodeEntityAdapter.getParameter());
        Assert.assertNull(specNodeEntityAdapter.getInputContexts());
        Assert.assertEquals(specNodeEntityAdapter.getOutputContexts(), Collections.emptyList());
        Assert.assertEquals(specNodeEntityAdapter.getInnerNodes(), Collections.emptyList());
        Assert.assertNull(specNodeEntityAdapter.getTaskRerunTime());
        Assert.assertNull(specNodeEntityAdapter.getTaskRerunInterval());
        Assert.assertEquals(specNodeEntityAdapter.getDependentType(), (Integer)DependentType.NONE.getValue());
        Assert.assertNull(specNodeEntityAdapter.getCycleType());
        Assert.assertNull(specNodeEntityAdapter.getLastModifyTime());
        Assert.assertNull(specNodeEntityAdapter.getLastModifyUser());
        Assert.assertNull(specNodeEntityAdapter.getMultiInstCheckType());
        Assert.assertNull(specNodeEntityAdapter.getPriority());
        Assert.assertNull(specNodeEntityAdapter.getDependentDataNode());
        Assert.assertNull(specNodeEntityAdapter.getOwnerName());
        Assert.assertNull(specNodeEntityAdapter.getExtraConfig());
        Assert.assertNull(specNodeEntityAdapter.getExtraContent());
        Assert.assertNull(specNodeEntityAdapter.getTtContent());
        Assert.assertNull(specNodeEntityAdapter.getAdvanceSettings());
        Assert.assertNull(specNodeEntityAdapter.getExtend());
        Assert.assertNull(specNodeEntityAdapter.getComponent());
        Assert.assertNull(specNodeEntityAdapter.getImageId());
        Assert.assertNull(specNodeEntityAdapter.getCalendarId());
        Assert.assertNull(specNodeEntityAdapter.getStreamLaunchMode());
        Assert.assertNull(specNodeEntityAdapter.getIgnoreBranchConditionSkip());
        Assert.assertNull(specNodeEntityAdapter.getParentId());
        Assert.assertNull(specNodeEntityAdapter.getCu());
        Assert.assertNull(specNodeEntityAdapter.getOrigin());
        Assert.assertNull(specNodeEntityAdapter.getWorkflowName());
        Assert.assertNull(specNodeEntityAdapter.getConfigPack());
    }

    @Test
    public void testSpecFileResourceEntityAdapter() {
        SpecFileResource specFileResource = new SpecFileResource();
        specFileResource.setId("123");
        specFileResource.setName("test_resource");
        specFileResource.setType(SpecFileResourceType.FILE);

        SpecScript script = new SpecScript();
        script.setPath("path/to/script");
        script.setContent("script content");
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setCommand(CodeProgramType.ODPS_JAR.getName());
        script.setRuntime(runtime);
        specFileResource.setScript(script);

        SpecRuntimeResource runtimeResource = new SpecRuntimeResource();
        runtimeResource.setResourceGroup("test_resource_group");
        specFileResource.setRuntimeResource(runtimeResource);

        SpecDatasource datasource = new SpecDatasource();
        datasource.setId("111");
        datasource.setName("test_connection");
        specFileResource.setDatasource(datasource);

        SpecFileResourceEntityAdapter specFileResourceEntityAdapter = new SpecFileResourceEntityAdapter();
        specFileResourceEntityAdapter.setSpecFileResource(specFileResource);

        Assert.assertEquals(specFileResourceEntityAdapter.getUuid(), specFileResource.getId());
        Assert.assertEquals(specFileResourceEntityAdapter.getName(), specFileResource.getName());
        Assert.assertNull(specFileResourceEntityAdapter.getDescription());
        Assert.assertNull(specFileResourceEntityAdapter.getOwner());
        Assert.assertEquals(specFileResourceEntityAdapter.getFolder(), "path/to");
        Assert.assertEquals(specFileResourceEntityAdapter.getCode(), script.getContent());
        Assert.assertEquals(specFileResourceEntityAdapter.getType(), runtime.getCommand());
        Assert.assertEquals(specFileResourceEntityAdapter.getResourceGroup(), runtimeResource.getResourceGroup());
        Assert.assertEquals(specFileResourceEntityAdapter.getResourceGroupName(), runtimeResource.getResourceGroup());
        Assert.assertEquals(specFileResourceEntityAdapter.getConnection(), datasource.getName());

        Assert.assertNull(specFileResourceEntityAdapter.getBizId());
        Assert.assertNull(specFileResourceEntityAdapter.getBizName());
        Assert.assertEquals(specFileResourceEntityAdapter.getTypeId(), (Integer)CodeProgramType.ODPS_JAR.getCode());
        Assert.assertNull(specFileResourceEntityAdapter.getCronExpress());
        Assert.assertNull(specFileResourceEntityAdapter.getStartEffectDate());
        Assert.assertNull(specFileResourceEntityAdapter.getEndEffectDate());
        Assert.assertNull(specFileResourceEntityAdapter.getDiResourceGroup());
        Assert.assertNull(specFileResourceEntityAdapter.getDiResourceGroupName());
        Assert.assertNull(specFileResourceEntityAdapter.getCodeMode());
        Assert.assertNull(specFileResourceEntityAdapter.getStartRightNow());
        Assert.assertNull(specFileResourceEntityAdapter.getRerunMode());
        Assert.assertNull(specFileResourceEntityAdapter.getNodeType());
        Assert.assertNull(specFileResourceEntityAdapter.getPauseSchedule());
        Assert.assertEquals(specFileResourceEntityAdapter.getIsAutoParse(), Integer.valueOf(0));
        Assert.assertNull(specFileResourceEntityAdapter.getNodeUseType());
        Assert.assertNull(specFileResourceEntityAdapter.getRef());
        Assert.assertNull(specFileResourceEntityAdapter.getRoot());
        Assert.assertNull(specFileResourceEntityAdapter.getParameter());
        Assert.assertEquals(specFileResourceEntityAdapter.getInputContexts(), Collections.emptyList());
        Assert.assertEquals(specFileResourceEntityAdapter.getOutputContexts(), Collections.emptyList());
        Assert.assertEquals(specFileResourceEntityAdapter.getInnerNodes(), Collections.emptyList());
        Assert.assertNull(specFileResourceEntityAdapter.getTaskRerunTime());
        Assert.assertNull(specFileResourceEntityAdapter.getTaskRerunInterval());
        Assert.assertNull(specFileResourceEntityAdapter.getDependentType());
        Assert.assertNull(specFileResourceEntityAdapter.getCycleType());
        Assert.assertNull(specFileResourceEntityAdapter.getLastModifyTime());
        Assert.assertNull(specFileResourceEntityAdapter.getLastModifyUser());
        Assert.assertNull(specFileResourceEntityAdapter.getMultiInstCheckType());
        Assert.assertNull(specFileResourceEntityAdapter.getPriority());
        Assert.assertNull(specFileResourceEntityAdapter.getDependentDataNode());
        Assert.assertNull(specFileResourceEntityAdapter.getOwnerName());
        Assert.assertNull(specFileResourceEntityAdapter.getExtraConfig());
        Assert.assertNull(specFileResourceEntityAdapter.getExtraContent());
        Assert.assertNull(specFileResourceEntityAdapter.getTtContent());
        Assert.assertNull(specFileResourceEntityAdapter.getAdvanceSettings());
        Assert.assertNull(specFileResourceEntityAdapter.getExtend());
        Assert.assertNull(specFileResourceEntityAdapter.getComponent());
        Assert.assertNull(specFileResourceEntityAdapter.getImageId());
        Assert.assertNull(specFileResourceEntityAdapter.getCalendarId());
        Assert.assertNull(specFileResourceEntityAdapter.getStreamLaunchMode());
        Assert.assertNull(specFileResourceEntityAdapter.getIgnoreBranchConditionSkip());
        Assert.assertNull(specFileResourceEntityAdapter.getParentId());
        Assert.assertNull(specFileResourceEntityAdapter.getCu());
        Assert.assertNull(specFileResourceEntityAdapter.getOrigin());
        Assert.assertNull(specFileResourceEntityAdapter.getWorkflowName());
        Assert.assertNull(specFileResourceEntityAdapter.getConfigPack());
    }

    @Test
    public void testSpecFunctionEntityAdapter() {
        // 创建并填充 SpecFunction 对象
        SpecFunction specFunction = new SpecFunction();
        specFunction.setId("123");
        specFunction.setName("test_function");
        specFunction.setType(FunctionType.MATH);
        specFunction.setClassName("com.example.TestFunction");

        SpecScript script = new SpecScript();
        script.setPath("path/to/script");
        script.setContent("function content");
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setCommand(CodeProgramType.ODPS_FUNCTION.getName());
        script.setRuntime(runtime);
        specFunction.setScript(script);

        SpecRuntimeResource runtimeResource = new SpecRuntimeResource();
        runtimeResource.setResourceGroup("test_resource_group");
        specFunction.setRuntimeResource(runtimeResource);

        SpecDatasource datasource = new SpecDatasource();
        datasource.setId("111");
        datasource.setName("test_connection");
        specFunction.setDatasource(datasource);

        SpecFunctionEntityAdapter specFunctionEntityAdapter = new SpecFunctionEntityAdapter();
        specFunctionEntityAdapter.setSpecFunction(specFunction);

        // 验证 SpecFunctionEntityAdapter 的属性
        Assert.assertEquals(specFunctionEntityAdapter.getUuid(), specFunction.getId());
        Assert.assertEquals(specFunctionEntityAdapter.getName(), specFunction.getName());
        Assert.assertNull(specFunctionEntityAdapter.getDescription());
        Assert.assertNull(specFunctionEntityAdapter.getOwner());
        Assert.assertEquals(specFunctionEntityAdapter.getFolder(), "path/to");
        Assert.assertEquals(specFunctionEntityAdapter.getCode(), script.getContent());
        Assert.assertEquals(specFunctionEntityAdapter.getType(), runtime.getCommand());
        Assert.assertEquals(specFunctionEntityAdapter.getResourceGroup(), runtimeResource.getResourceGroup());
        Assert.assertEquals(specFunctionEntityAdapter.getResourceGroupName(), runtimeResource.getResourceGroup());
        Assert.assertEquals(specFunctionEntityAdapter.getConnection(), datasource.getName());

        Assert.assertNull(specFunctionEntityAdapter.getBizId());
        Assert.assertNull(specFunctionEntityAdapter.getBizName());
        Assert.assertEquals(specFunctionEntityAdapter.getTypeId(), (Integer)CodeProgramType.ODPS_FUNCTION.getCode());
        Assert.assertNull(specFunctionEntityAdapter.getCronExpress());
        Assert.assertNull(specFunctionEntityAdapter.getStartEffectDate());
        Assert.assertNull(specFunctionEntityAdapter.getEndEffectDate());
        Assert.assertNull(specFunctionEntityAdapter.getDiResourceGroup());
        Assert.assertNull(specFunctionEntityAdapter.getDiResourceGroupName());
        Assert.assertNull(specFunctionEntityAdapter.getCodeMode());
        Assert.assertNull(specFunctionEntityAdapter.getStartRightNow());
        Assert.assertNull(specFunctionEntityAdapter.getRerunMode());
        Assert.assertNull(specFunctionEntityAdapter.getNodeType());
        Assert.assertNull(specFunctionEntityAdapter.getPauseSchedule());
        Assert.assertEquals(specFunctionEntityAdapter.getIsAutoParse(), Integer.valueOf(0));
        Assert.assertNull(specFunctionEntityAdapter.getNodeUseType());
        Assert.assertNull(specFunctionEntityAdapter.getRef());
        Assert.assertNull(specFunctionEntityAdapter.getRoot());
        Assert.assertNull(specFunctionEntityAdapter.getParameter());
        Assert.assertEquals(specFunctionEntityAdapter.getInputContexts(), Collections.emptyList());
        Assert.assertEquals(specFunctionEntityAdapter.getOutputContexts(), Collections.emptyList());
        Assert.assertEquals(specFunctionEntityAdapter.getInnerNodes(), Collections.emptyList());
        Assert.assertNull(specFunctionEntityAdapter.getTaskRerunTime());
        Assert.assertNull(specFunctionEntityAdapter.getTaskRerunInterval());
        Assert.assertNull(specFunctionEntityAdapter.getDependentType());
        Assert.assertNull(specFunctionEntityAdapter.getCycleType());
        Assert.assertNull(specFunctionEntityAdapter.getLastModifyTime());
        Assert.assertNull(specFunctionEntityAdapter.getLastModifyUser());
        Assert.assertNull(specFunctionEntityAdapter.getMultiInstCheckType());
        Assert.assertNull(specFunctionEntityAdapter.getPriority());
        Assert.assertNull(specFunctionEntityAdapter.getDependentDataNode());
        Assert.assertNull(specFunctionEntityAdapter.getOwnerName());
        Assert.assertNull(specFunctionEntityAdapter.getExtraConfig());
        Assert.assertNull(specFunctionEntityAdapter.getExtraContent());
        Assert.assertNull(specFunctionEntityAdapter.getTtContent());
        Assert.assertNull(specFunctionEntityAdapter.getAdvanceSettings());
        Assert.assertNull(specFunctionEntityAdapter.getExtend());
        Assert.assertNull(specFunctionEntityAdapter.getComponent());
        Assert.assertNull(specFunctionEntityAdapter.getImageId());
        Assert.assertNull(specFunctionEntityAdapter.getCalendarId());
        Assert.assertNull(specFunctionEntityAdapter.getStreamLaunchMode());
        Assert.assertNull(specFunctionEntityAdapter.getIgnoreBranchConditionSkip());
        Assert.assertNull(specFunctionEntityAdapter.getParentId());
        Assert.assertEquals(specFunctionEntityAdapter.getCu(), runtime.getCu());
        Assert.assertNull(specFunctionEntityAdapter.getOrigin());
        Assert.assertNull(specFunctionEntityAdapter.getWorkflowName());
        Assert.assertNull(specFunctionEntityAdapter.getConfigPack());
    }

}
