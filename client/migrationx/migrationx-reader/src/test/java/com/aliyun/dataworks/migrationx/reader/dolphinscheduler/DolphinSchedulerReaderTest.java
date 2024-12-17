/*
 * Copyright (c) 2024, Alibaba Cloud;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.dataworks.migrationx.reader.dolphinscheduler;

import java.io.File;
import java.util.Collections;
import java.util.List;

import com.aliyun.dataworks.common.spec.utils.ReflectUtils;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.DolphinSchedulerApiService;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.PaginateData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.PaginateResponse;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.Response;

import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

/**
 * @author 聿剑
 * @date 2024/5/23
 */

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class DolphinSchedulerReaderTest {
    @Test
    public void testReader() throws Exception {
        String endpoint = "http://endpoint";
        String token = "token";
        String version = "1.3.9";
        List<String> projects = Collections.singletonList("project1");
        DolphinSchedulerApiService service = Mockito.mock(DolphinSchedulerApiService.class);
        Response<List<JsonObject>> projectResponse = new Response<>();
        JsonObject projJson = new JsonObject();
        projJson.addProperty("name", "project1");
        projJson.addProperty("code", "project1");
        projectResponse.setData(Collections.singletonList(projJson));
        Mockito.when(service.queryAllProjectList(Mockito.any())).thenReturn(projectResponse);
        Response<List<JsonObject>> resourceResponse = new Response<>();
        resourceResponse.setData(Collections.emptyList());
        //Mockito.when(service.queryResourceList(Mockito.any())).thenReturn(resourceResponse);
        PaginateResponse<JsonObject> functionResponse = new PaginateResponse<>();
        PaginateData<JsonObject> funcData = new PaginateData<>();
        funcData.setTotalPage(0).setTotal(0).setTotalList(Collections.emptyList());
        functionResponse.setData(funcData);
        Mockito.when(service.queryUdfFuncListByPaging(Mockito.any())).thenReturn(functionResponse);
        File exportFile = new File(new File(DolphinSchedulerReaderTest.class.getClassLoader().getResource("").getFile()), "export.zip");
        DolphinSchedulerReader reader = new DolphinSchedulerReader(endpoint, token, version, projects, Collections.emptyList(), exportFile);
        ReflectUtils.setFieldValue(reader, "dolphinSchedulerApiService", service);
        reader.export();
        Assert.assertTrue(exportFile.exists());
    }

    @Test
    public void testParseProjects_Normal() {
        try (MockedConstruction<DolphinSchedulerReader> mockTank = mockConstruction(DolphinSchedulerReader.class, (mock, context) -> {
            Assert.assertEquals("http://1.1.1.1:12345/", context.arguments().get(0));
            Assert.assertEquals("xxx", context.arguments().get(1));
            Assert.assertEquals("3.1.5", context.arguments().get(2));
            Assert.assertEquals("p1", ((List) context.arguments().get(3)).get(0));
            Assert.assertEquals(2, ((List) context.arguments().get(3)).size());
            Assert.assertEquals("p2", ((List) context.arguments().get(3)).get(1));
            Assert.assertEquals(0, ((List) context.arguments().get(4)).size());
            Assert.assertEquals((new File("test1")).getAbsolutePath(), ((File) context.arguments().get(5)).getAbsolutePath());
            when(mock.export()).thenReturn(new File("."));
        })) {
            DolphinSchedulerCommandApp dolphinSchedulerReader = new DolphinSchedulerCommandApp();
            String[] args = new String[]{
                    "-e", "http://1.1.1.1:12345/",
                    "-t", "xxx",
                    "-v", "3.1.5",
                    "-p", "p1,p2",
                    "-f", "test1"
            };
            dolphinSchedulerReader.run(args);
        } catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void testParseProjects_Codes() {
        try (MockedConstruction<DolphinSchedulerReader> mockTank = mockConstruction(DolphinSchedulerReader.class, (mock, context) -> {
            Assert.assertEquals("http://1.1.1.1:12345/", context.arguments().get(0));
            Assert.assertEquals("xxx", context.arguments().get(1));
            Assert.assertEquals("3.1.5", context.arguments().get(2));
            Assert.assertEquals(0, ((List) context.arguments().get(3)).size());
            Assert.assertEquals(2, ((List) context.arguments().get(4)).size());
            Assert.assertEquals(Long.valueOf(12345L), ((List) context.arguments().get(4)).get(0));
            Assert.assertEquals(Long.valueOf(123468L), ((List) context.arguments().get(4)).get(1));
            Assert.assertEquals((new File("test1")).getAbsolutePath(), ((File) context.arguments().get(5)).getAbsolutePath());
            when(mock.export()).thenReturn(new File("."));
        })) {
            DolphinSchedulerCommandApp dolphinSchedulerReader = new DolphinSchedulerCommandApp();
            String[] args = new String[]{
                    "-e", "http://1.1.1.1:12345/",
                    "-t", "xxx",
                    "-v", "3.1.5",
                    "-p", "code:12345,123468",
                    "-f", "test1"
            };
            dolphinSchedulerReader.run(args);
        } catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void testParseProjects_Names() {
        try (MockedConstruction<DolphinSchedulerReader> mockTank = mockConstruction(DolphinSchedulerReader.class, (mock, context) -> {
            Assert.assertEquals("http://1.1.1.1:12345/", context.arguments().get(0));
            Assert.assertEquals("xxx", context.arguments().get(1));
            Assert.assertEquals("3.1.5", context.arguments().get(2));
            Assert.assertEquals(2, ((List) context.arguments().get(3)).size());
            Assert.assertEquals("p1", ((List) context.arguments().get(3)).get(0));
            Assert.assertEquals("p2", ((List) context.arguments().get(3)).get(1));
            Assert.assertEquals(0, ((List) context.arguments().get(4)).size());
            Assert.assertEquals((new File("test1")).getAbsolutePath(), ((File) context.arguments().get(5)).getAbsolutePath());
            when(mock.export()).thenReturn(new File("."));
        })) {
            DolphinSchedulerCommandApp dolphinSchedulerReader = new DolphinSchedulerCommandApp();
            String[] args = new String[]{
                    "-e", "http://1.1.1.1:12345/",
                    "-t", "xxx",
                    "-v", "3.1.5",
                    "-p", "name:p1,p2",
                    "-f", "test1"
            };
            dolphinSchedulerReader.run(args);
        } catch (Exception e) {
            throw e;
        }
    }
}
