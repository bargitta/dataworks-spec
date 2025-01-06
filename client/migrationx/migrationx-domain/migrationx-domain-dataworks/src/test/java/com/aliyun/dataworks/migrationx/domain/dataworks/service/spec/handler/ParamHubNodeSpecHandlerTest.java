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

package com.aliyun.dataworks.migrationx.domain.dataworks.service.spec.handler;

import java.util.Arrays;

import com.aliyun.dataworks.common.spec.adapter.SpecHandlerContext;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DwNode;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.NodeContext;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.types.NodeUseType;
import com.aliyun.dataworks.migrationx.domain.dataworks.service.spec.entity.DwNodeEntityAdapter;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author 聿剑
 * @date 2024/6/18
 */
public class ParamHubNodeSpecHandlerTest {
    @Test
    public void test() {
        ParamHubNodeSpecHandler paramHubNodeSpecHandler = new ParamHubNodeSpecHandler();
        paramHubNodeSpecHandler.setContext(new SpecHandlerContext());
        DwNode paramHub = new DwNode();
        paramHub.setName("param_hub");
        paramHub.setNodeUseType(NodeUseType.SCHEDULED);
        paramHub.setType(CodeProgramType.DIDE_SHELL.getName());
        NodeContext out1 = new NodeContext();
        out1.setParamName("out1");
        out1.setOutput("out1");
        out1.setParamType(1);
        out1.setParamValue("out1");
        out1.setParseType(1);
        out1.setType(1);
        NodeContext out2 = new NodeContext();
        out2.setParamName("out2");
        out2.setOutput("out2");
        out2.setParamType(1);
        out2.setParamValue("out2");
        out2.setParseType(1);
        out2.setType(1);
        paramHub.setOutputContexts(Arrays.asList(out1, out2));

        SpecNode specNode = paramHubNodeSpecHandler.handle(new DwNodeEntityAdapter(paramHub));
        Assert.assertNotNull(specNode);
        Assert.assertNotNull(specNode.getParamHub());
        Assert.assertEquals(2, CollectionUtils.size(specNode.getParamHub().getVariables()));
    }
}
