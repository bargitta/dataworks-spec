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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import com.alibaba.fastjson2.JSONObject;

import org.apache.commons.lang3.StringUtils;

/**
 * spark-submit command and adb spark json conversion
 * spark-submit command example:
 * <pre>{@code
 * bin/spark-submit
 *   --keyId STS.***RBVag
 *   --secretId 9p1w***riLdK
 *   --stsToken ***5oIAA=
 *   --clusterId amv-uf***2mn
 *   --rgName spark
 *   --endpoint adb.cn-shanghai.aliyuncs.com
 *   --conf spark.sql.cbo.enabled=true
 *   --conf spark.dynamicAllocation.enabled=true
 *   oss://adb-spark-regulation-cn-shanghai/code/python/oss_integration.py
 *   adb-spark-regulation-cn-shanghai
 *   data/target
 * }</pre>
 * json example:
 * <pre>{@code
 * {
 *     "comments": [
 *         "-- Here is just an example of SparkPi. Modify the content and run your spark program."
 *     ],
 *     "args": [
 *         "1000"
 *     ],
 *     "file": "local:///tmp/spark-examples.jar",
 *     "name": "SparkPi",
 *     "className": "org.apache.spark.examples.SparkPi",
 *     "conf": {
 *         "spark.driver.resourceSpec": "medium",
 *         "spark.executor.instances": 2,
 *         "spark.executor.resourceSpec": "medium"
 *     }
 * }
 * }</pre>
 * ```
 *
 * @date 2024/9/25
 */
public class SparkSubmitCommandUtils {
    public static Pattern SPARK_SUBMIT_PATTERN = Pattern.compile("[\\s|\\n|\\t]*spark-submit.*");

    public static JSONObject parseCommandToJson(String command) {
        JSONObject json = new JSONObject();
        if (StringUtils.isBlank(command)) {
            return json;
        }

        List<String> args = new ArrayList<>(Arrays.asList(StringUtils.split(command)));
        if (!args.isEmpty() && SPARK_SUBMIT_PATTERN.matcher(args.get(0)).find()) {
            args = args.subList(1, args.size());
        }
        SparkSubmitCommandBuilder builder = new SparkSubmitCommandBuilder(args);

        json.put("args", builder.getAppArgs());
        json.put("file", builder.getAppResource());
        json.put("name", builder.getAppName());
        json.put("className", builder.getMainClass());
        json.put("conf", Optional.ofNullable(builder.getConf()).map(JSONObject::new).orElse(null));
        json.put("jars", builder.getJars());
        json.put("pyFiles", builder.getPyFiles());
        json.put("customArgs", builder.getCustomArgs());
        return json;
    }

    public static String parseJsonToCommand(String jsonStr) {
        JSONObject json = JSONObject.parseObject(jsonStr);
        SparkSubmitCommandBuilder builder = new SparkSubmitCommandBuilder();
        builder.setAppName(json.getString("name"));
        builder.setAppResource(json.getString("file"));
        builder.setMainClass(json.getString("className"));
        Optional.ofNullable(json.getJSONObject("conf")).ifPresent(conf ->
            conf.keySet().forEach(key -> builder.getConf().put(key, conf.getString(key))));
        builder.setAppName(builder.getAppName());
        Optional.ofNullable(json.getJSONArray("args")).ifPresent(args -> args.forEach(arg -> builder.getAppArgs().add(arg.toString())));
        Optional.ofNullable(json.getJSONArray("jars")).ifPresent(jars -> jars.forEach(jar -> builder.getJars().add(jar.toString())));
        Optional.ofNullable(json.getJSONArray("pyFiles")).ifPresent(
            pyFiles -> pyFiles.forEach(pyFile -> builder.getPyFiles().add(pyFile.toString())));
        Optional.ofNullable(json.getJSONObject("customArgs")).ifPresent(customArgs ->
            customArgs.forEach((key, value) -> builder.getCustomArgs().put(key, String.valueOf(value))));
        List<String> args = new ArrayList<>(Collections.singletonList("spark-submit"));
        args.addAll(builder.buildSparkSubmitArgs());
        return String.join(" ", args);
    }
}
