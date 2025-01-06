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

package com.aliyun.dataworks.client.utils.spark.command;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONWriter.Feature;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author 聿剑
 * @date 2024/9/26
 */
public class SparkSubmitCommandUtilsTest {
    @Test
    public void testCommandToJson() {
        String command = "bin/spark-submit\n"
            + "  --keyId STS.***RBVag\n"
            + "  --secretId 9p1w***riLdK\n"
            + "  --stsToken ***5oIAA=\n"
            + "  --clusterId amv-uf***2mn\n"
            + "  --callerParentUid 1234466887\n"
            + "  --callerType CUSTOMER\n"
            + "  --callerUid 09888128182\n"
            + "  --rgName spark\n"
            + "  --endpoint adb.cn-shanghai.aliyuncs.com\n"
            + "  --conf spark.sql.cbo.enabled=true\n"
            + "  --conf spark.dynamicAllocation.enabled=true\n"
            + "  --conf spark.executor.instances=2\n"
            + "  --jars oss://adb/pyfiles/adb-spark-regulation-cn-shanghai.jar,oss://adb/pyfiles/data/target1.jar\n"
            + "  --class org.apache.spark.examples.SparkPi \n"
            + "  --master spark://207.184.161.138:7077 \n"
            + "  --deploy-mode cluster \n"
            + "  --supervise \n"
            + "  --executor-memory 20G \n"
            + "  --total-executor-cores 100 \n"
            + "  --name test-spark-py-pi \n"
            + " oss://adb-spark-regulation-cn-shanghai/code/python/oss_integration.jar\n"
            + " adb-spark-regulation-cn-shanghai\n"
            + " data/target";
        JSONObject json = SparkSubmitCommandUtils.parseCommandToJson(command);
        System.out.println(json.toJSONString(Feature.PrettyFormat));
        /*
          {
          	"args":[
          		"adb-spark-regulation-cn-shanghai",
          		"data/target"
          	],
          	"file":"oss://adb-spark-regulation-cn-shanghai/code/python/oss_integration.py",
          	"name":"test-spark-py-pi",
          	"className":"org.apache.spark.examples.SparkPi",
          	"conf":{
          		"spark.executor.memory":"20G",
          		"spark.sql.cbo.enabled":"true",
          		"spark.dynamicAllocation.enabled":"true"
            }
          }
         */
        Assert.assertNotNull(json.getJSONArray("args"));
        Assert.assertArrayEquals(new String[] {"adb-spark-regulation-cn-shanghai", "data/target"}, json.getJSONArray("args").toArray());
        Assert.assertArrayEquals(new String[] {"oss://adb/pyfiles/adb-spark-regulation-cn-shanghai.jar", "oss://adb/pyfiles/data/target1.jar"},
            json.getJSONArray("jars").toArray());
        Assert.assertEquals("oss://adb-spark-regulation-cn-shanghai/code/python/oss_integration.jar", json.getString("file"));
        Assert.assertEquals("test-spark-py-pi", json.getString("name"));
        Assert.assertEquals("org.apache.spark.examples.SparkPi", json.getString("className"));
        Assert.assertTrue(json.containsKey("conf"));
        Assert.assertEquals("20G", json.getJSONObject("conf").getString("spark.executor.memory"));
        Assert.assertEquals("true", json.getJSONObject("conf").getString("spark.sql.cbo.enabled"));
        Assert.assertEquals("true", json.getJSONObject("conf").getString("spark.dynamicAllocation.enabled"));
        Assert.assertEquals("adb.cn-shanghai.aliyuncs.com", json.getJSONObject("customArgs").getString("--endpoint"));
        Assert.assertEquals("09888128182", json.getJSONObject("customArgs").getString("--callerUid"));
    }

    @Test
    public void testJsonToCommand() {
        String json = "{\n"
            + "          \t\"args\":[\n"
            + "          \t\t\"adb-spark-regulation-cn-shanghai\",\n"
            + "          \t\t\"data/target\"\n"
            + "          \t],\n"
            + "          \t\"file\":\"oss://adb-spark-regulation-cn-shanghai/code/python/oss_integration.py\",\n"
            + "          \t\"pyFiles\":[\n"
            + "          \t\t\"oss://adb/pyfiles/adb-spark-regulation-cn-shanghai.py\",\n"
            + "          \t\t\"oss://adb/pyfiles/data/target1.py\"\n"
            + "          \t],\n"
            + "          \t\"name\":\"test-spark-py-pi\",\n"
            + "          \t\"className\":\"org.apache.spark.examples.SparkPi\",\n"
            + "          \t\"conf\":{\n"
            + "          \t\t\"spark.executor.memory\":\"20G\",\n"
            + "          \t\t\"spark.sql.cbo.enabled\":\"true\",\n"
            + "          \t\t\"spark.dynamicAllocation.enabled\":\"true\"\n"
            + "          }\n"
            + "}";

        JSONObject jsonObject = JSONObject.parseObject(json);
        String customArgs = "{\n"
            + "\t\t\"--endpoint\":\"adb.cn-shanghai.aliyuncs.com\",\n"
            + "\t\t\"--callerUid\":\"09888128182\",\n"
            + "\t\t\"--rgName\":\"spark\",\n"
            + "\t\t\"--callerParentUid\":\"1234466887\",\n"
            + "\t\t\"--secretId\":\"9p1w***riLdK\",\n"
            + "\t\t\"--keyId\":\"STS.***RBVag\",\n"
            + "\t\t\"--stsToken\":\"***5oIAA=\",\n"
            + "\t\t\"--total-executor-cores\":\"100\",\n"
            + "\t\t\"--clusterId\":\"amv-uf***2mn\",\n"
            + "\t\t\""
            + "--callerType\":\"CUSTOMER\"\n"
            + "\t}";
        jsonObject.put("customArgs", JSONObject.parseObject(customArgs));
        String cmd = SparkSubmitCommandUtils.parseJsonToCommand(jsonObject.toJSONString());
        System.out.println(cmd);
        Assert.assertEquals(
            "spark-submit --name test-spark-py-pi --conf spark.executor.memory=20G --conf spark.sql.cbo.enabled=true --conf spark.dynamicAllocation"
                + ".enabled=true --py-files oss://adb/pyfiles/adb-spark-regulation-cn-shanghai.py,oss://adb/pyfiles/data/target1.py --class org"
                + ".apache.spark.examples.SparkPi --endpoint adb.cn-shanghai.aliyuncs.com --callerUid 09888128182 --rgName spark --callerParentUid "
                + "1234466887 --secretId 9p1w***riLdK --keyId STS.***RBVag --stsToken ***5oIAA= --total-executor-cores 100 --clusterId amv-uf***2mn"
                + " --callerType CUSTOMER oss://adb-spark-regulation-cn-shanghai/code/python/oss_integration.py "
                + "adb-spark-regulation-cn-shanghai data/target",
            cmd);
    }
}
