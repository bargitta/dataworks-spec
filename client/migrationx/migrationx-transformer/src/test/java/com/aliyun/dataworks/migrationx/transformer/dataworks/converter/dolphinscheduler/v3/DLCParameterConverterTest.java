package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DolphinSchedulerV3Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.DataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.Project;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.UdfFunc;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.ResourceInfo;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DwNode;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DwWorkflow;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.DolphinSchedulerConverterContext;
import com.aliyun.migrationx.common.context.TransformerContext;
import com.aliyun.migrationx.common.utils.JSONUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DLCParameterConverterTest {

    @Test
    public void testDLCParameterConverter() throws Exception {
        Process process = Mockito.mock(Process.class);
        InputStream inputStream = Mockito.mock(InputStream.class);
        String json = IOUtils.resourceToString("/json/dolphin/v3/dolphin_dlc_v3.json", StandardCharsets.UTF_8);
        List<DagData> dagData = JSONUtils.parseObject(json, new TypeReference<List<DagData>>() {});

        try (MockedConstruction<ProcessBuilder> mockTank = mockConstruction(ProcessBuilder.class, (mock, context) -> {
            when(mock.redirectErrorStream(true)).thenReturn(Mockito.mock(ProcessBuilder.class));
            when(mock.start()).thenReturn(process);
            when(process.getInputStream()).thenReturn(inputStream);
        }); MockedStatic<IOUtils> utilities = Mockito.mockStatic(IOUtils.class)) {
            utilities.when(() -> IOUtils.readLines(inputStream, StandardCharsets.UTF_8)).thenReturn(Arrays.asList("test"));
            DolphinSchedulerConverterContext<Project, DagData, DataSource, ResourceInfo,
                    UdfFunc> converterContext = new DolphinSchedulerConverterContext<>();
            Properties properties = new Properties();
            converterContext.setProperties(properties);
            DwWorkflow dwWorkflow = new DwWorkflow();
            dwWorkflow.setNodes(new ArrayList<>());
            converterContext.setDwWorkflow(dwWorkflow);
            com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.Project project = new com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.Project();
            converterContext.setProject(project);
            DagData processMeta = dagData.get(0);
            project.setName("test1");
            processMeta.getProcessDefinition().setProjectName("test1");
            TaskDefinition taskDefinition = processMeta.getTaskDefinitionList().get(0);
            DolphinSchedulerV3Context.initContext(null, dagData, null, null, null);
            TransformerContext.getContext().setCustomResourceDir("src/test/resources/py2sql");
            TransformerContext.getContext().setScriptDir("src/main/python/python2sql");
            DLCParameterConverter converter = new DLCParameterConverter(processMeta, taskDefinition, converterContext);
            List<DwNode> nodes = converter.doConvert();
            Assert.assertNotNull(nodes);
            Assert.assertTrue(nodes.size() > 0);
        } catch (Exception e) {
            throw e;
        }
    }
}