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

package com.aliyun.dataworks.migrationx.dolphinscheduler.v1;

import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.DolphinSchedulerApiService;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v1.QueryProcessDefinitionByPaginateRequest;
import com.aliyun.migrationx.common.http.HttpClientUtil;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

/**
 * @author 聿剑
 * @date 2022/10/20
 */
@RunWith(MockitoJUnitRunner.class)
public class DolphinSchedulerApiServiceTest {

    @Test
    public void testParseProjects_Normal() throws Exception {
        try (MockedConstruction<HttpClientUtil> mockTank = mockConstruction(HttpClientUtil.class, (mock, context) -> {
            when(mock.executeAndGet(any())).thenReturn("");
        })) {
            DolphinSchedulerApiService service = new DolphinSchedulerApiService("", "");
            QueryProcessDefinitionByPaginateRequest request = new QueryProcessDefinitionByPaginateRequest();
            service.queryProcessDefinitionByPaging(request);
        } catch (Exception e) {
            throw e;
        }
    }
}
