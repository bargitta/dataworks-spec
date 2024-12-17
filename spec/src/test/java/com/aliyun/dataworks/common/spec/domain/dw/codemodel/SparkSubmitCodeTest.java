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

package com.aliyun.dataworks.common.spec.domain.dw.codemodel;

import com.alibaba.fastjson2.JSONObject;

import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author 聿剑
 * @date 2024/9/25
 */
public class SparkSubmitCodeTest {
    @Test
    public void test() {
        JSONObject jsonObject = new JSONObject();
        String json = "{\n"
            + "          \t\"args\":[\n"
            + "          \t\t\"adb-spark-regulation-cn-shanghai\",\n"
            + "          \t\t\"data/target\"\n"
            + "          \t],\n"
            + "          \t\"file\":\"oss://adb-spark-regulation-cn-shanghai/code/python/oss_integration.py\",\n"
            + "          \t\"name\":\"test-spark-py-pi\",\n"
            + "          \t\"className\":\"org.apache.spark.examples.SparkPi\",\n"
            + "          \t\"conf\":{\n"
            + "          \t\t\"spark.executor.memory\":\"20G\",\n"
            + "          \t\t\"spark.sql.cbo.enabled\":\"true\",\n"
            + "          \t\t\"spark.dynamicAllocation.enabled\":\"true\"\n"
            + "            }\n"
            + "          }";
        jsonObject.fluentPut("command", "spark-submit --master yarn").fluentPut("json", JSONObject.parseObject(json));
        CodeModel<SparkSubmitCode> code = CodeModelFactory.getCodeModel(CodeProgramType.ADB_SPARK.name(), jsonObject.toJSONString());
        System.out.println(code.getRawContent());

        JSONObject contentJs = JSONObject.parseObject(code.getRawContent());
        Assert.assertNotNull(contentJs);
        Assert.assertNotNull(contentJs.getJSONObject("json"));
        Assert.assertNotNull(contentJs.getString("command"));
    }
}
