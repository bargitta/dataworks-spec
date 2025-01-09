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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;

import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.utils.GsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author 聿剑
 * @date 2022/12/28
 */
@Slf4j
public class CodeModelFactoryTest {
    @Test
    public void testGetCodeModel() {
        CodeModel<OdpsSparkCode> odpsSparkCode = CodeModelFactory.getCodeModel("ODPS_SPARK", "{}");
        odpsSparkCode.getCodeModel().setResourceReferences(new ArrayList<>());
        odpsSparkCode.getCodeModel().getResourceReferences().add("test_res.jar");
        System.out.println("code content: " + odpsSparkCode.getCodeModel().getContent());
        Assert.assertTrue(StringUtils.indexOf(
            odpsSparkCode.getContent(), "##@resource_reference{\"test_res.jar\"}") >= 0);
        Assert.assertNotNull(odpsSparkCode.getCodeModel());
        Assert.assertNotNull(odpsSparkCode.getCodeModel().getSparkJson());

        CodeModel<EmrCode> emr = CodeModelFactory.getCodeModel("EMR_HIVE", "");
        emr.getCodeModel().setName("emr");
        emr.getCodeModel().setType(EmrJobType.HIVE);
        emr.getCodeModel().setProperties(new EmrProperty().setArguments(Collections.singletonList("hive -e")));
        System.out.println("emr code: " + emr.getCodeModel().getContent());
        Assert.assertTrue(StringUtils.indexOf(emr.getContent(), "hive -e") >= 0);

        CodeModel<ControllerJoinCode> joinCode = CodeModelFactory.getCodeModel("CONTROLLER_JOIN", null);
        joinCode.getCodeModel()
            .setBranchList(Collections.singletonList(
                new ControllerJoinCode.Branch()
                    .setLogic(0)
                    .setNode("branch_node")
                    .setRunStatus(Arrays.asList("1", "2"))))
            .setResultStatus("2");
        System.out.println("join code: " + joinCode.getCodeModel().getContent());
        Assert.assertNotNull(joinCode.getCodeModel());
        Assert.assertTrue(StringUtils.indexOf(joinCode.getContent(), "branch_node") >= 0);

        CodeModel<PlainTextCode> odpsSqlCode = CodeModelFactory.getCodeModel("ODPS_SQL", "select 1;");
        System.out.println("odps code: " + odpsSqlCode.getContent());
        Assert.assertNotNull(odpsSqlCode);
        Assert.assertEquals("select 1;", odpsSqlCode.getContent());

        Assert.assertNotNull(CodeModelFactory.getCodeModel(null, null));

        CodeModel<Code> emrHive = CodeModelFactory.getCodeModel("EMR_HIVE", null);
        System.out.println("emr hive code: " + emrHive.getContent());
        EmrCode code = (EmrCode)emrHive.getCodeModel();
        System.out.println("emr code mode: {}" + GsonUtils.toJsonString(code));
    }

    @Test
    public void testMultiLanguageCode() {
        CodeModel<MultiLanguageScriptingCode> m = CodeModelFactory.getCodeModel("CONTROLLER_ASSIGNMENT", "");
        m.getCodeModel().setSourceCode("select 1");
        m.getCodeModel().setLanguage("odps");

        CodeModel<MultiLanguageScriptingCode> m2 = CodeModelFactory.getCodeModel("CONTROLLER_CYCLE_END", "{\n"
            + "  \"language\": \"odps\",\n"
            + "  \"content\": \"select 1\"\n"
            + "}");
        System.out.println("assignment content: " + m.getContent());
        System.out.println("assignment source code: " + m.getSourceCode());
        Assert.assertEquals("select 1", m.getSourceCode());
        System.out.println("cycle end content: " + m2.getContent());
        System.out.println("cycle end source code: " + m2.getSourceCode());
        Assert.assertEquals("select 1", m2.getSourceCode());

        System.out.println("template: " + m2.getTemplate());

        CodeModel<EmrCode> c = CodeModelFactory.getCodeModel("EMR_HIVE", "");
        c.setSourceCode("select 1");
        System.out.println("emr hive template: " + GsonUtils.toJsonString(c.getTemplate()));
    }

    @Test
    public void testDefaultJsonFormCode() {
        String code = "{\n"
            + "            \"content\": \"IMPORT FOREIGN SCHEMA shanghai_onlineTest_simple LIMIT TO (wq_test_dataworks_pt_001) from SERVER "
            + "odps_server INTO public OPTIONS(prefix 'tmp_foreign_', suffix 'xozi4mmb', if_table_exist 'error',if_unsupported_type 'error');"
            + "\\nDROP TABLE IF EXISTS \\\"public\\\".tmp_holo_8gwvxopb_wqtest;\\nBEGIN;\\nCREATE TABLE IF NOT EXISTS \\\"public\\\""
            + ".tmp_holo_8gwvxopb_wqtest (\\n \\\"f1\\\" text NOT NULL,\\n \\\"f2\\\" text NOT NULL,\\n \\\"f4\\\" text NOT NULL,\\n \\\"f5\\\" "
            + "text NOT NULL,\\n \\\"f3\\\" text NOT NULL,\\n \\\"f6\\\" text NOT NULL,\\n \\\"f7\\\" text NOT NULL,\\n \\\"f10\\\" text NOT NULL,"
            + "\\n \\\"ds\\\" bigint NOT NULL,\\n \\\"pt\\\" text NOT NULL\\n);\\nCALL SET_TABLE_PROPERTY('\\\"public\\\""
            + ".tmp_holo_8gwvxopb_wqtest', 'orientation', 'column');\\ncomment on column \\\"public\\\".tmp_holo_8gwvxopb_wqtest.pt is '分区字段';"
            + "\\nCOMMIT;\\nINSERT INTO \\\"public\\\".tmp_holo_8gwvxopb_wqtest\\nSELECT \\n    CAST(\\\"f1\\\" as text),\\n    CAST(\\\"f2\\\" as "
            + "text),\\n    CAST(\\\"f4\\\" as text),\\n    CAST(\\\"f5\\\" as text),\\n    CAST(\\\"f3\\\" as text),\\n    CAST(\\\"f6\\\" as "
            + "text),\\n    CAST(\\\"f7\\\" as text),\\n    CAST(\\\"f10\\\" as text),\\n    CAST(\\\"ds\\\" as bigint),\\n    CAST(\\\"pt\\\" as "
            + "text)\\nFROM \\\"public\\\".tmp_foreign_wq_test_dataworks_pt_001xozi4mmb\\nWHERE pt='${bizdate}';\\nDROP FOREIGN TABLE IF EXISTS "
            + "\\\"public\\\".tmp_foreign_wq_test_dataworks_pt_001xozi4mmb;BEGIN;\\nDROP TABLE IF EXISTS \\\"public\\\".wqtest;\\nALTER TABLE "
            + "\\\"public\\\".tmp_holo_8gwvxopb_wqtest RENAME TO wqtest;\\nCOMMIT;\\n\",\n"
            + "            \"extraContent\": \"{\\\"connId\\\":\\\"yongxunqa_holo_shanghai\\\",\\\"dbName\\\":\\\"yongxunqa_hologres_db\\\","
            + "\\\"syncType\\\":1,\\\"extendProjectName\\\":\\\"shanghai_onlineTest_simple\\\",\\\"schemaName\\\":\\\"public\\\","
            + "\\\"tableName\\\":\\\"wqtest\\\",\\\"partitionColumn\\\":\\\"\\\",\\\"orientation\\\":\\\"column\\\","
            + "\\\"columns\\\":[{\\\"name\\\":\\\"f1\\\",\\\"comment\\\":\\\"\\\",\\\"type\\\":\\\"STRING\\\",\\\"allowNull\\\":false,"
            + "\\\"holoName\\\":\\\"f1\\\",\\\"holoType\\\":\\\"text\\\"},{\\\"name\\\":\\\"f2\\\",\\\"comment\\\":\\\"\\\","
            + "\\\"type\\\":\\\"STRING\\\",\\\"allowNull\\\":false,\\\"holoName\\\":\\\"f2\\\",\\\"holoType\\\":\\\"text\\\"},"
            + "{\\\"name\\\":\\\"f4\\\",\\\"comment\\\":\\\"\\\",\\\"type\\\":\\\"STRING\\\",\\\"allowNull\\\":false,\\\"holoName\\\":\\\"f4\\\","
            + "\\\"holoType\\\":\\\"text\\\"},{\\\"name\\\":\\\"f5\\\",\\\"comment\\\":\\\"\\\",\\\"type\\\":\\\"STRING\\\","
            + "\\\"allowNull\\\":false,\\\"holoName\\\":\\\"f5\\\",\\\"holoType\\\":\\\"text\\\"},{\\\"name\\\":\\\"f3\\\","
            + "\\\"comment\\\":\\\"\\\",\\\"type\\\":\\\"STRING\\\",\\\"allowNull\\\":false,\\\"holoName\\\":\\\"f3\\\","
            + "\\\"holoType\\\":\\\"text\\\"},{\\\"name\\\":\\\"f6\\\",\\\"comment\\\":\\\"\\\",\\\"type\\\":\\\"STRING\\\","
            + "\\\"allowNull\\\":false,\\\"holoName\\\":\\\"f6\\\",\\\"holoType\\\":\\\"text\\\"},{\\\"name\\\":\\\"f7\\\","
            + "\\\"comment\\\":\\\"\\\",\\\"type\\\":\\\"STRING\\\",\\\"allowNull\\\":false,\\\"holoName\\\":\\\"f7\\\","
            + "\\\"holoType\\\":\\\"text\\\"},{\\\"name\\\":\\\"f10\\\",\\\"comment\\\":\\\"\\\",\\\"type\\\":\\\"STRING\\\","
            + "\\\"allowNull\\\":false,\\\"holoName\\\":\\\"f10\\\",\\\"holoType\\\":\\\"text\\\"},{\\\"name\\\":\\\"ds\\\","
            + "\\\"comment\\\":\\\"\\\",\\\"type\\\":\\\"BIGINT\\\",\\\"allowNull\\\":false,\\\"holoName\\\":\\\"ds\\\","
            + "\\\"holoType\\\":\\\"bigint\\\"},{\\\"name\\\":\\\"pt\\\",\\\"comment\\\":\\\"分区字段\\\",\\\"type\\\":\\\"STRING\\\","
            + "\\\"allowNull\\\":false,\\\"holoName\\\":\\\"pt\\\",\\\"holoType\\\":\\\"text\\\"}],\\\"serverName\\\":\\\"odps_server\\\","
            + "\\\"extendTableName\\\":\\\"wq_test_dataworks_pt_001\\\",\\\"foreignSchemaName\\\":\\\"public\\\",\\\"foreignTableName\\\":\\\"\\\","
            + "\\\"instanceId\\\":\\\"yongxunqa_holo_shanghai\\\",\\\"engineType\\\":\\\"Hologres\\\",\\\"clusteringKey\\\":[],"
            + "\\\"bitmapIndexKey\\\":[],\\\"segmentKey\\\":[],\\\"dictionaryEncoding\\\":[]}\"\n"
            + "        }";
        CodeModel<DefaultJsonFormCode> codeModel = CodeModelFactory.getCodeModel(CodeProgramType.HOLOGRES_SYNC_DATA.name(), code);
        log.info("content: {}", codeModel.getCodeModel().getContent());
        log.info("extraContent: {}", codeModel.getCodeModel().getExtraContent());
        Assert.assertTrue(StringUtils.startsWith(codeModel.getCodeModel().getContent(), "IMPORT FOREIGN SCHEMA shanghai_on"));

        CodeModel<DefaultJsonFormCode> newCode = CodeModelFactory.getCodeModel(CodeProgramType.HOLOGRES_SYNC_DATA.name(), "{}");
        newCode.getCodeModel().setExtraContent("xx").setContent("select 1");
        log.info("content: {}", newCode.getContent());
        log.info("rawContent: {}", newCode.getRawContent());
        Assert.assertEquals("xx", newCode.getCodeModel().getExtraContent());
        Assert.assertEquals("select 1", newCode.getContent());
        Assert.assertNotNull(newCode.getRawContent());
    }

    @Test
    public void testEmrCodeJobTypeDefault() {
        CodeModel<EmrCode> emr = CodeModelFactory.getCodeModel("EMR_HIVE", "");
        emr.getCodeModel().setProperties(new EmrProperty().setArguments(Collections.singletonList("hive -e")));
        emr.getCodeModel().setType(EmrJobType.HIVE_SQL);
        System.out.println("emr code: " + emr.getCodeModel().getContent());
        Assert.assertTrue(StringUtils.indexOf(emr.getContent(), "hive -e") >= 0);
        Assert.assertEquals(EmrJobType.HIVE_SQL, emr.getCodeModel().getType());

        String originalCode = "{\n"
            + "  \"type\": \"TRINO_SQL\",\n"
            + "  \"launcher\": {\n"
            + "    \"allocationSpec\": {}\n"
            + "  },\n"
            + "  \"properties\": {\n"
            + "    \"envs\": {},\n"
            + "    \"arguments\": [\n"
            + "      \"hive -e\"\n"
            + "    ],\n"
            + "    \"tags\": []\n"
            + "  },\n"
            + "  \"programType\": \"EMR_HIVE\"\n"
            + "}";

        // if emr job type is set in code json, it will be used by ignoring the CodeProgramType
        emr = CodeModelFactory.getCodeModel("EMR_HIVE", originalCode);
        Assert.assertNotNull(emr.getCodeModel());
        Assert.assertEquals(EmrJobType.TRINO_SQL, emr.getCodeModel().getType());
    }

    @Test
    public void testEscapeHtml() {
        CodeModel<EmrCode> code = CodeModelFactory.getCodeModel(CodeProgramType.EMR_SPARK.name(), null);
        code.getCodeModel().setSourceCode(
            "spark-submit --deploy-mode cluster --class org.apache.spark.examples.SparkPi http://schedule@{env}inside.cheetah.alibaba-inc"
                + ".com/scheduler/res?id=282842366");

        log.info("content: {}", code.getCodeModel().getContent());

        Assert.assertTrue(StringUtils.indexOf(code.getCodeModel().getContent(), code.getCodeModel().getSourceCode()) > 0);
    }

    @Test
    public void testPaiDesigner() {
        String code
            = "{\"content\":\"{\\\"appId\\\":609609,\\\"computeResource\\\":{\\\"MaxCompute\\\":\\\"execution_maxcompute\\\"},"
            + "\\\"connectionType\\\":\\\"MaxCompute\\\",\\\"description\\\":\\\"机器学习算法计算出二氧化氮对于雾霾影响最大。\\\","
            + "\\\"flowUniqueCode\\\":\\\"42d966e5-20c3-4685-9ed2-de52d01912f9\\\",\\\"inputs\\\":[{\\\"type\\\":\\\"MaxComputeTable\\\","
            + "\\\"value\\\":\\\"pai_online_project.wumai_data\\\"}],\\\"name\\\":\\\"up雾霾天气预测001\\\",\\\"outputs\\\":[],"
            + "\\\"paiflowArguments\\\":\\\"---\\\\narguments:\\\\n  parameters:\\\\n  - name: \\\\\\\"execution_maxcompute\\\\\\\"\\\\n    "
            + "value:\\\\n      endpoint: \\\\\\\"http://service.cn-shanghai.maxcompute.aliyun-inc.com/api\\\\\\\"\\\\n      odpsProject: "
            + "\\\\\\\"mc1205004\\\\\\\"\\\\n      spec:\\\\n        endpoint: \\\\\\\"http://service.cn-shanghai.maxcompute.aliyun-inc"
            + ".com/api\\\\\\\"\\\\n        odpsProject: \\\\\\\"mc1205004\\\\\\\"\\\\n      resourceType: \\\\\\\"MaxCompute\\\\\\\"\\\\n\\\","
            + "\\\"paiflowParameters\\\":{},\\\"paiflowPipeline\\\":\\\"---\\\\napiVersion: \\\\\\\"core/v1\\\\\\\"\\\\nmetadata:\\\\n  provider: "
            + "\\\\\\\"1107550004253538\\\\\\\"\\\\n  version: \\\\\\\"v1\\\\\\\"\\\\n  identifier: "
            + "\\\\\\\"job-root-pipeline-identifier\\\\\\\"\\\\n  annotations: {}\\\\nspec:\\\\n  inputs:\\\\n    artifacts: []\\\\n    "
            + "parameters:\\\\n    - name: \\\\\\\"execution_maxcompute\\\\\\\"\\\\n      type: \\\\\\\"Map\\\\\\\"\\\\n  arguments:\\\\n    "
            + "artifacts: []\\\\n    parameters: []\\\\n  dependencies: []\\\\n  initContainers: []\\\\n  sideCarContainers: []\\\\n  "
            + "pipelines:\\\\n  - apiVersion: \\\\\\\"core/v1\\\\\\\"\\\\n    metadata:\\\\n      provider: \\\\\\\"pai\\\\\\\"\\\\n      version: "
            + "\\\\\\\"v1\\\\\\\"\\\\n      identifier: \\\\\\\"logisticregression_binary\\\\\\\"\\\\n      name: "
            + "\\\\\\\"id-9531-1608891073887-61143\\\\\\\"\\\\n      displayName: \\\\\\\"逻辑回归二分类\\\\\\\"\\\\n      annotations: {}\\\\n    "
            + "spec:\\\\n      arguments:\\\\n        artifacts:\\\\n        - name: \\\\\\\"inputTable\\\\\\\"\\\\n          from: "
            + "\\\\\\\"{{pipelines.id-85ae-1608984392467-81946.outputs.artifacts.output1Table}}\\\\\\\"\\\\n        parameters:\\\\n        - name:"
            + " \\\\\\\"featureColNames\\\\\\\"\\\\n          value: \\\\\\\"pm10,so2,co,no2\\\\\\\"\\\\n        - name: "
            + "\\\\\\\"labelColName\\\\\\\"\\\\n          value: \\\\\\\"_c2\\\\\\\"\\\\n        - name: \\\\\\\"goodValue\\\\\\\"\\\\n          "
            + "value: \\\\\\\"1\\\\\\\"\\\\n        - name: \\\\\\\"enableSparse\\\\\\\"\\\\n          value: \\\\\\\"false\\\\\\\"\\\\n        - "
            + "name: \\\\\\\"generatePmml\\\\\\\"\\\\n          value: false\\\\n        - name: \\\\\\\"roleArn\\\\\\\"\\\\n          value: "
            + "\\\\\\\"acs:ram::1107550004253538:role/aliyunodpspaidefaultrole\\\\\\\"\\\\n        - name: \\\\\\\"regularizedType\\\\\\\"\\\\n    "
            + "      value: \\\\\\\"None\\\\\\\"\\\\n        - name: \\\\\\\"maxIter\\\\\\\"\\\\n          value: \\\\\\\"100\\\\\\\"\\\\n        -"
            + " name: \\\\\\\"regularizedLevel\\\\\\\"\\\\n          value: \\\\\\\"1\\\\\\\"\\\\n        - name: \\\\\\\"epsilon\\\\\\\"\\\\n     "
            + "     value: \\\\\\\"0.000001\\\\\\\"\\\\n        - name: \\\\\\\"execution\\\\\\\"\\\\n          from: \\\\\\\"{{inputs.parameters"
            + ".execution_maxcompute}}\\\\\\\"\\\\n      dependencies:\\\\n      - \\\\\\\"id-85ae-1608984392467-81946\\\\\\\"\\\\n      "
            + "initContainers: []\\\\n      sideCarContainers: []\\\\n      pipelines: []\\\\n      volumes: []\\\\n  - apiVersion: "
            + "\\\\\\\"core/v1\\\\\\\"\\\\n    metadata:\\\\n      provider: \\\\\\\"pai\\\\\\\"\\\\n      version: \\\\\\\"v1\\\\\\\"\\\\n      "
            + "identifier: \\\\\\\"random_forests_1\\\\\\\"\\\\n      name: \\\\\\\"id-3d25-1608980864737-77856\\\\\\\"\\\\n      displayName: "
            + "\\\\\\\"随机森林\\\\\\\"\\\\n      annotations: {}\\\\n    spec:\\\\n      arguments:\\\\n        artifacts:\\\\n        - name: "
            + "\\\\\\\"inputTable\\\\\\\"\\\\n          from: \\\\\\\"{{pipelines.id-85ae-1608984392467-81946.outputs.artifacts"
            + ".output1Table}}\\\\\\\"\\\\n        parameters:\\\\n        - name: \\\\\\\"featureColNames\\\\\\\"\\\\n          value: "
            + "\\\\\\\"pm10,so2,co,no2\\\\\\\"\\\\n        - name: \\\\\\\"labelColName\\\\\\\"\\\\n          value: \\\\\\\"_c2\\\\\\\"\\\\n      "
            + "  - name: \\\\\\\"generatePmml\\\\\\\"\\\\n          value: false\\\\n        - name: \\\\\\\"roleArn\\\\\\\"\\\\n          value: "
            + "\\\\\\\"acs:ram::1107550004253538:role/aliyunodpspaidefaultrole\\\\\\\"\\\\n        - name: \\\\\\\"treeNum\\\\\\\"\\\\n          "
            + "value: \\\\\\\"100\\\\\\\"\\\\n        - name: \\\\\\\"minNumObj\\\\\\\"\\\\n          value: \\\\\\\"2\\\\\\\"\\\\n        - name: "
            + "\\\\\\\"minNumPer\\\\\\\"\\\\n          value: \\\\\\\"0\\\\\\\"\\\\n        - name: \\\\\\\"maxRecordSize\\\\\\\"\\\\n          "
            + "value: \\\\\\\"100000\\\\\\\"\\\\n        - name: \\\\\\\"execution\\\\\\\"\\\\n          from: \\\\\\\"{{inputs.parameters"
            + ".execution_maxcompute}}\\\\\\\"\\\\n      dependencies:\\\\n      - \\\\\\\"id-85ae-1608984392467-81946\\\\\\\"\\\\n      "
            + "initContainers: []\\\\n      sideCarContainers: []\\\\n      pipelines: []\\\\n      volumes: []\\\\n  - apiVersion: "
            + "\\\\\\\"core/v1\\\\\\\"\\\\n    metadata:\\\\n      provider: \\\\\\\"pai\\\\\\\"\\\\n      version: \\\\\\\"v1\\\\\\\"\\\\n      "
            + "identifier: \\\\\\\"type_transform\\\\\\\"\\\\n      name: \\\\\\\"id-2d88-1608982098027-91558\\\\\\\"\\\\n      displayName: "
            + "\\\\\\\"类型转换\\\\\\\"\\\\n      annotations: {}\\\\n    spec:\\\\n      arguments:\\\\n        artifacts:\\\\n        - name: "
            + "\\\\\\\"inputTable\\\\\\\"\\\\n          from: \\\\\\\"{{pipelines.id-9309-1608992715826-47326.outputs.artifacts"
            + ".outputTable}}\\\\\\\"\\\\n        parameters:\\\\n        - name: \\\\\\\"cols_to_double\\\\\\\"\\\\n          value: \\\\\\\"time,"
            + "hour,pm2,pm10,so2,co,no2\\\\\\\"\\\\n        - name: \\\\\\\"default_int_value\\\\\\\"\\\\n          value: \\\\\\\"0\\\\\\\"\\\\n  "
            + "      - name: \\\\\\\"reserveOldFeat\\\\\\\"\\\\n          value: \\\\\\\"false\\\\\\\"\\\\n        - name: "
            + "\\\\\\\"execution\\\\\\\"\\\\n          from: \\\\\\\"{{inputs.parameters.execution_maxcompute}}\\\\\\\"\\\\n      "
            + "dependencies:\\\\n      - \\\\\\\"id-9309-1608992715826-47326\\\\\\\"\\\\n      initContainers: []\\\\n      sideCarContainers: "
            + "[]\\\\n      pipelines: []\\\\n      volumes: []\\\\n  - apiVersion: \\\\\\\"core/v1\\\\\\\"\\\\n    metadata:\\\\n      provider: "
            + "\\\\\\\"pai\\\\\\\"\\\\n      version: \\\\\\\"v1\\\\\\\"\\\\n      identifier: \\\\\\\"fe_meta_runner\\\\\\\"\\\\n      name: "
            + "\\\\\\\"id-2317-1608984201281-74996\\\\\\\"\\\\n      displayName: \\\\\\\"数据视图\\\\\\\"\\\\n      annotations: {}\\\\n    spec:\\\\n"
            + "      arguments:\\\\n        artifacts:\\\\n        - name: \\\\\\\"inputTable\\\\\\\"\\\\n          from: \\\\\\\"{{pipelines"
            + ".id-73af-1608994414936-19117.outputs.artifacts.outputTable}}\\\\\\\"\\\\n        parameters:\\\\n        - name: "
            + "\\\\\\\"selectedCols\\\\\\\"\\\\n          value: \\\\\\\"pm10,so2,co,no2\\\\\\\"\\\\n        - name: \\\\\\\"labelCol\\\\\\\"\\\\n "
            + "         value: \\\\\\\"_c2\\\\\\\"\\\\n        - name: \\\\\\\"isSparse\\\\\\\"\\\\n          value: \\\\\\\"false\\\\\\\"\\\\n    "
            + "    - name: \\\\\\\"maxBins\\\\\\\"\\\\n          value: \\\\\\\"100\\\\\\\"\\\\n        - name: \\\\\\\"execution\\\\\\\"\\\\n     "
            + "     from: \\\\\\\"{{inputs.parameters.execution_maxcompute}}\\\\\\\"\\\\n      dependencies:\\\\n      - "
            + "\\\\\\\"id-73af-1608994414936-19117\\\\\\\"\\\\n      initContainers: []\\\\n      sideCarContainers: []\\\\n      pipelines: "
            + "[]\\\\n      volumes: []\\\\n  - apiVersion: \\\\\\\"core/v1\\\\\\\"\\\\n    metadata:\\\\n      provider: \\\\\\\"pai\\\\\\\"\\\\n "
            + "     version: \\\\\\\"v1\\\\\\\"\\\\n      identifier: \\\\\\\"split\\\\\\\"\\\\n      name: "
            + "\\\\\\\"id-85ae-1608984392467-81946\\\\\\\"\\\\n      displayName: \\\\\\\"拆分\\\\\\\"\\\\n      annotations: {}\\\\n    spec:\\\\n  "
            + "    arguments:\\\\n        artifacts:\\\\n        - name: \\\\\\\"inputTable\\\\\\\"\\\\n          from: \\\\\\\"{{pipelines"
            + ".id-9869-1608986331830-78910.outputs.artifacts.outputTable}}\\\\\\\"\\\\n        parameters:\\\\n        - name: "
            + "\\\\\\\"_splitMethod\\\\\\\"\\\\n          value: \\\\\\\"_fraction\\\\\\\"\\\\n        - name: \\\\\\\"fraction\\\\\\\"\\\\n       "
            + "   value: \\\\\\\"0.8\\\\\\\"\\\\n        - name: \\\\\\\"_advanced\\\\\\\"\\\\n          value: \\\\\\\"false\\\\\\\"\\\\n        -"
            + " name: \\\\\\\"execution\\\\\\\"\\\\n          from: \\\\\\\"{{inputs.parameters.execution_maxcompute}}\\\\\\\"\\\\n      "
            + "dependencies:\\\\n      - \\\\\\\"id-9869-1608986331830-78910\\\\\\\"\\\\n      initContainers: []\\\\n      sideCarContainers: "
            + "[]\\\\n      pipelines: []\\\\n      volumes: []\\\\n  - apiVersion: \\\\\\\"core/v1\\\\\\\"\\\\n    metadata:\\\\n      provider: "
            + "\\\\\\\"pai\\\\\\\"\\\\n      version: \\\\\\\"v1\\\\\\\"\\\\n      identifier: \\\\\\\"Prediction_1\\\\\\\"\\\\n      name: "
            + "\\\\\\\"id-4191-1608984751235-85036\\\\\\\"\\\\n      displayName: \\\\\\\"预测\\\\\\\"\\\\n      annotations: {}\\\\n    spec:\\\\n  "
            + "    arguments:\\\\n        artifacts:\\\\n        - name: \\\\\\\"model\\\\\\\"\\\\n          from: \\\\\\\"{{pipelines"
            + ".id-3d25-1608980864737-77856.outputs.artifacts.model}}\\\\\\\"\\\\n        - name: \\\\\\\"inputTable\\\\\\\"\\\\n          from: "
            + "\\\\\\\"{{pipelines.id-85ae-1608984392467-81946.outputs.artifacts.output2Table}}\\\\\\\"\\\\n        parameters:\\\\n        - name:"
            + " \\\\\\\"featureColNames\\\\\\\"\\\\n          value: \\\\\\\"pm10,so2,co,no2\\\\\\\"\\\\n        - name: "
            + "\\\\\\\"appendColNames\\\\\\\"\\\\n          value: \\\\\\\"time,hour,pm10,so2,co,no2,_c2\\\\\\\"\\\\n        - name: "
            + "\\\\\\\"resultColName\\\\\\\"\\\\n          value: \\\\\\\"prediction_result\\\\\\\"\\\\n        - name: "
            + "\\\\\\\"scoreColName\\\\\\\"\\\\n          value: \\\\\\\"prediction_score\\\\\\\"\\\\n        - name: "
            + "\\\\\\\"detailColName\\\\\\\"\\\\n          value: \\\\\\\"prediction_detail\\\\\\\"\\\\n        - name: "
            + "\\\\\\\"enableSparse\\\\\\\"\\\\n          value: \\\\\\\"false\\\\\\\"\\\\n        - name: \\\\\\\"execution\\\\\\\"\\\\n          "
            + "from: \\\\\\\"{{inputs.parameters.execution_maxcompute}}\\\\\\\"\\\\n      dependencies:\\\\n      - "
            + "\\\\\\\"id-3d25-1608980864737-77856\\\\\\\"\\\\n      - \\\\\\\"id-85ae-1608984392467-81946\\\\\\\"\\\\n      initContainers: "
            + "[]\\\\n      sideCarContainers: []\\\\n      pipelines: []\\\\n      volumes: []\\\\n  - apiVersion: \\\\\\\"core/v1\\\\\\\"\\\\n   "
            + " metadata:\\\\n      provider: \\\\\\\"pai\\\\\\\"\\\\n      version: \\\\\\\"v1\\\\\\\"\\\\n      identifier: "
            + "\\\\\\\"Prediction_1\\\\\\\"\\\\n      name: \\\\\\\"id-6b26-1608984755520-07203\\\\\\\"\\\\n      displayName: "
            + "\\\\\\\"预测\\\\\\\"\\\\n      annotations: {}\\\\n    spec:\\\\n      arguments:\\\\n        artifacts:\\\\n        - name: "
            + "\\\\\\\"inputTable\\\\\\\"\\\\n          from: \\\\\\\"{{pipelines.id-85ae-1608984392467-81946.outputs.artifacts"
            + ".output2Table}}\\\\\\\"\\\\n        - name: \\\\\\\"model\\\\\\\"\\\\n          from: \\\\\\\"{{pipelines"
            + ".id-9531-1608891073887-61143.outputs.artifacts.model}}\\\\\\\"\\\\n        parameters:\\\\n        - name: "
            + "\\\\\\\"featureColNames\\\\\\\"\\\\n          value: \\\\\\\"pm10,so2,co,no2\\\\\\\"\\\\n        - name: "
            + "\\\\\\\"appendColNames\\\\\\\"\\\\n          value: \\\\\\\"time,hour,pm10,so2,co,no2,_c2\\\\\\\"\\\\n        - name: "
            + "\\\\\\\"resultColName\\\\\\\"\\\\n          value: \\\\\\\"prediction_result\\\\\\\"\\\\n        - name: "
            + "\\\\\\\"scoreColName\\\\\\\"\\\\n          value: \\\\\\\"prediction_score\\\\\\\"\\\\n        - name: "
            + "\\\\\\\"detailColName\\\\\\\"\\\\n          value: \\\\\\\"prediction_detail\\\\\\\"\\\\n        - name: "
            + "\\\\\\\"enableSparse\\\\\\\"\\\\n          value: \\\\\\\"false\\\\\\\"\\\\n        - name: \\\\\\\"execution\\\\\\\"\\\\n          "
            + "from: \\\\\\\"{{inputs.parameters.execution_maxcompute}}\\\\\\\"\\\\n      dependencies:\\\\n      - "
            + "\\\\\\\"id-85ae-1608984392467-81946\\\\\\\"\\\\n      - \\\\\\\"id-9531-1608891073887-61143\\\\\\\"\\\\n      initContainers: "
            + "[]\\\\n      sideCarContainers: []\\\\n      pipelines: []\\\\n      volumes: []\\\\n  - apiVersion: \\\\\\\"core/v1\\\\\\\"\\\\n   "
            + " metadata:\\\\n      provider: \\\\\\\"pai\\\\\\\"\\\\n      version: \\\\\\\"v1\\\\\\\"\\\\n      identifier: "
            + "\\\\\\\"evaluate_1\\\\\\\"\\\\n      name: \\\\\\\"id-5dd4-1608984770722-54098\\\\\\\"\\\\n      displayName: "
            + "\\\\\\\"二分类评估\\\\\\\"\\\\n      annotations: {}\\\\n    spec:\\\\n      arguments:\\\\n        artifacts:\\\\n        - name: "
            + "\\\\\\\"inputTable\\\\\\\"\\\\n          from: \\\\\\\"{{pipelines.id-4191-1608984751235-85036.outputs.artifacts"
            + ".outputTable}}\\\\\\\"\\\\n        parameters:\\\\n        - name: \\\\\\\"labelColName\\\\\\\"\\\\n          value: "
            + "\\\\\\\"_c2\\\\\\\"\\\\n        - name: \\\\\\\"scoreColName\\\\\\\"\\\\n          value: \\\\\\\"prediction_score\\\\\\\"\\\\n     "
            + "   - name: \\\\\\\"positiveLabel\\\\\\\"\\\\n          value: \\\\\\\"1\\\\\\\"\\\\n        - name: \\\\\\\"binCount\\\\\\\"\\\\n   "
            + "       value: \\\\\\\"1000\\\\\\\"\\\\n        - name: \\\\\\\"_advanced\\\\\\\"\\\\n          value: \\\\\\\"false\\\\\\\"\\\\n    "
            + "    - name: \\\\\\\"execution\\\\\\\"\\\\n          from: \\\\\\\"{{inputs.parameters.execution_maxcompute}}\\\\\\\"\\\\n      "
            + "dependencies:\\\\n      - \\\\\\\"id-4191-1608984751235-85036\\\\\\\"\\\\n      initContainers: []\\\\n      sideCarContainers: "
            + "[]\\\\n      pipelines: []\\\\n      volumes: []\\\\n  - apiVersion: \\\\\\\"core/v1\\\\\\\"\\\\n    metadata:\\\\n      provider: "
            + "\\\\\\\"pai\\\\\\\"\\\\n      version: \\\\\\\"v1\\\\\\\"\\\\n      identifier: \\\\\\\"evaluate_1\\\\\\\"\\\\n      name: "
            + "\\\\\\\"id-41c6-1608984773170-26269\\\\\\\"\\\\n      displayName: \\\\\\\"二分类评估\\\\\\\"\\\\n      annotations: {}\\\\n    "
            + "spec:\\\\n      arguments:\\\\n        artifacts:\\\\n        - name: \\\\\\\"inputTable\\\\\\\"\\\\n          from: "
            + "\\\\\\\"{{pipelines.id-6b26-1608984755520-07203.outputs.artifacts.outputTable}}\\\\\\\"\\\\n        parameters:\\\\n        - name: "
            + "\\\\\\\"labelColName\\\\\\\"\\\\n          value: \\\\\\\"_c2\\\\\\\"\\\\n        - name: \\\\\\\"scoreColName\\\\\\\"\\\\n         "
            + " value: \\\\\\\"prediction_score\\\\\\\"\\\\n        - name: \\\\\\\"positiveLabel\\\\\\\"\\\\n          value: "
            + "\\\\\\\"1\\\\\\\"\\\\n        - name: \\\\\\\"binCount\\\\\\\"\\\\n          value: \\\\\\\"1000\\\\\\\"\\\\n        - name: "
            + "\\\\\\\"_advanced\\\\\\\"\\\\n          value: \\\\\\\"false\\\\\\\"\\\\n        - name: \\\\\\\"execution\\\\\\\"\\\\n          "
            + "from: \\\\\\\"{{inputs.parameters.execution_maxcompute}}\\\\\\\"\\\\n      dependencies:\\\\n      - "
            + "\\\\\\\"id-6b26-1608984755520-07203\\\\\\\"\\\\n      initContainers: []\\\\n      sideCarContainers: []\\\\n      pipelines: "
            + "[]\\\\n      volumes: []\\\\n  - apiVersion: \\\\\\\"core/v1\\\\\\\"\\\\n    metadata:\\\\n      provider: \\\\\\\"pai\\\\\\\"\\\\n "
            + "     version: \\\\\\\"v1\\\\\\\"\\\\n      identifier: \\\\\\\"normalize_1\\\\\\\"\\\\n      name: "
            + "\\\\\\\"id-9869-1608986331830-78910\\\\\\\"\\\\n      displayName: \\\\\\\"归一化\\\\\\\"\\\\n      annotations: {}\\\\n    spec:\\\\n "
            + "     arguments:\\\\n        artifacts:\\\\n        - name: \\\\\\\"inputTable\\\\\\\"\\\\n          from: \\\\\\\"{{pipelines"
            + ".id-73af-1608994414936-19117.outputs.artifacts.outputTable}}\\\\\\\"\\\\n        parameters:\\\\n        - name: "
            + "\\\\\\\"selectedColNames\\\\\\\"\\\\n          value: \\\\\\\"pm10,so2,co,no2\\\\\\\"\\\\n        - name: "
            + "\\\\\\\"keepOriginal\\\\\\\"\\\\n          value: \\\\\\\"false\\\\\\\"\\\\n        - name: \\\\\\\"execution\\\\\\\"\\\\n          "
            + "from: \\\\\\\"{{inputs.parameters.execution_maxcompute}}\\\\\\\"\\\\n      dependencies:\\\\n      - "
            + "\\\\\\\"id-73af-1608994414936-19117\\\\\\\"\\\\n      initContainers: []\\\\n      sideCarContainers: []\\\\n      pipelines: "
            + "[]\\\\n      volumes: []\\\\n  - apiVersion: \\\\\\\"core/v1\\\\\\\"\\\\n    metadata:\\\\n      provider: \\\\\\\"pai\\\\\\\"\\\\n "
            + "     version: \\\\\\\"v1\\\\\\\"\\\\n      identifier: \\\\\\\"data_source\\\\\\\"\\\\n      name: "
            + "\\\\\\\"id-9309-1608992715826-47326\\\\\\\"\\\\n      displayName: \\\\\\\"读数据源\\\\\\\"\\\\n      annotations: {}\\\\n    spec:\\\\n"
            + "      arguments:\\\\n        artifacts: []\\\\n        parameters:\\\\n        - name: \\\\\\\"inputTableName\\\\\\\"\\\\n          "
            + "value: \\\\\\\"pai_online_project.wumai_data\\\\\\\"\\\\n        - name: \\\\\\\"execution\\\\\\\"\\\\n          from: "
            + "\\\\\\\"{{inputs.parameters.execution_maxcompute}}\\\\\\\"\\\\n      dependencies: []\\\\n      initContainers: []\\\\n      "
            + "sideCarContainers: []\\\\n      pipelines: []\\\\n      volumes: []\\\\n  - apiVersion: \\\\\\\"core/v1\\\\\\\"\\\\n    "
            + "metadata:\\\\n      provider: \\\\\\\"pai\\\\\\\"\\\\n      version: \\\\\\\"v1\\\\\\\"\\\\n      identifier: "
            + "\\\\\\\"sql\\\\\\\"\\\\n      name: \\\\\\\"id-73af-1608994414936-19117\\\\\\\"\\\\n      displayName: \\\\\\\"SQL组件\\\\\\\"\\\\n   "
            + "   annotations: {}\\\\n    spec:\\\\n      arguments:\\\\n        artifacts:\\\\n        - name: \\\\\\\"inputTable1\\\\\\\"\\\\n   "
            + "       from: \\\\\\\"{{pipelines.id-2d88-1608982098027-91558.outputs.artifacts.outputTable}}\\\\\\\"\\\\n        parameters:\\\\n   "
            + "     - name: \\\\\\\"sql\\\\\\\"\\\\n          value: \\\\\\\"select time,hour,(case when pm2>200 then 1 else 0 end),pm10,so2,co,"
            + "no2\\\\\\\\\\\\n            \\\\\\\\ from ${t1}\\\\\\\"\\\\n        - name: \\\\\\\"execution\\\\\\\"\\\\n          from: "
            + "\\\\\\\"{{inputs.parameters.execution_maxcompute}}\\\\\\\"\\\\n      dependencies:\\\\n      - "
            + "\\\\\\\"id-2d88-1608982098027-91558\\\\\\\"\\\\n      initContainers: []\\\\n      sideCarContainers: []\\\\n      pipelines: "
            + "[]\\\\n      volumes: []\\\\n  - apiVersion: \\\\\\\"core/v1\\\\\\\"\\\\n    metadata:\\\\n      provider: \\\\\\\"pai\\\\\\\"\\\\n "
            + "     version: \\\\\\\"v1\\\\\\\"\\\\n      identifier: \\\\\\\"histograms\\\\\\\"\\\\n      name: "
            + "\\\\\\\"id-3906-1608996028711-50772\\\\\\\"\\\\n      displayName: \\\\\\\"直方图\\\\\\\"\\\\n      annotations: {}\\\\n    spec:\\\\n "
            + "     arguments:\\\\n        artifacts:\\\\n        - name: \\\\\\\"inputTable\\\\\\\"\\\\n          from: \\\\\\\"{{pipelines"
            + ".id-2d88-1608982098027-91558.outputs.artifacts.outputTable}}\\\\\\\"\\\\n        parameters:\\\\n        - name: "
            + "\\\\\\\"selectedColNames\\\\\\\"\\\\n          value: \\\\\\\"hour,pm10,so2,co,no2,pm2\\\\\\\"\\\\n        - name: "
            + "\\\\\\\"intervalNum\\\\\\\"\\\\n          value: \\\\\\\"100\\\\\\\"\\\\n        - name: \\\\\\\"execution\\\\\\\"\\\\n          "
            + "from: \\\\\\\"{{inputs.parameters.execution_maxcompute}}\\\\\\\"\\\\n      dependencies:\\\\n      - "
            + "\\\\\\\"id-2d88-1608982098027-91558\\\\\\\"\\\\n      initContainers: []\\\\n      sideCarContainers: []\\\\n      pipelines: "
            + "[]\\\\n      volumes: []\\\\n  volumes: []\\\\n\\\",\\\"paraValue\\\":\\\"--paiflow_endpoint=paiflow.cn-shanghai.aliyuncs.com "
            + "--region=cn-shanghai\\\",\\\"prgType\\\":\\\"1000138\\\",\\\"requestId\\\":\\\"EBA73CBD-850E-53E4-BE5F-679410336F0C\\\","
            + "\\\"taskRelations\\\":[{\\\"childTaskUniqueCode\\\":\\\"id-3d25-1608980864737-77856\\\","
            + "\\\"parentTaskUniqueCode\\\":\\\"id-85ae-1608984392467-81946\\\"},{\\\"childTaskUniqueCode\\\":\\\"id-9531-1608891073887-61143\\\","
            + "\\\"parentTaskUniqueCode\\\":\\\"id-85ae-1608984392467-81946\\\"},{\\\"childTaskUniqueCode\\\":\\\"id-4191-1608984751235-85036\\\","
            + "\\\"parentTaskUniqueCode\\\":\\\"id-3d25-1608980864737-77856\\\"},{\\\"childTaskUniqueCode\\\":\\\"id-4191-1608984751235-85036\\\","
            + "\\\"parentTaskUniqueCode\\\":\\\"id-85ae-1608984392467-81946\\\"},{\\\"childTaskUniqueCode\\\":\\\"id-6b26-1608984755520-07203\\\","
            + "\\\"parentTaskUniqueCode\\\":\\\"id-85ae-1608984392467-81946\\\"},{\\\"childTaskUniqueCode\\\":\\\"id-6b26-1608984755520-07203\\\","
            + "\\\"parentTaskUniqueCode\\\":\\\"id-9531-1608891073887-61143\\\"},{\\\"childTaskUniqueCode\\\":\\\"id-5dd4-1608984770722-54098\\\","
            + "\\\"parentTaskUniqueCode\\\":\\\"id-4191-1608984751235-85036\\\"},{\\\"childTaskUniqueCode\\\":\\\"id-41c6-1608984773170-26269\\\","
            + "\\\"parentTaskUniqueCode\\\":\\\"id-6b26-1608984755520-07203\\\"},{\\\"childTaskUniqueCode\\\":\\\"id-85ae-1608984392467-81946\\\","
            + "\\\"parentTaskUniqueCode\\\":\\\"id-9869-1608986331830-78910\\\"},{\\\"childTaskUniqueCode\\\":\\\"id-2d88-1608982098027-91558\\\","
            + "\\\"parentTaskUniqueCode\\\":\\\"id-9309-1608992715826-47326\\\"},{\\\"childTaskUniqueCode\\\":\\\"id-73af-1608994414936-19117\\\","
            + "\\\"parentTaskUniqueCode\\\":\\\"id-2d88-1608982098027-91558\\\"},{\\\"childTaskUniqueCode\\\":\\\"id-2317-1608984201281-74996\\\","
            + "\\\"parentTaskUniqueCode\\\":\\\"id-73af-1608994414936-19117\\\"},{\\\"childTaskUniqueCode\\\":\\\"id-9869-1608986331830-78910\\\","
            + "\\\"parentTaskUniqueCode\\\":\\\"id-73af-1608994414936-19117\\\"},{\\\"childTaskUniqueCode\\\":\\\"id-3906-1608996028711-50772\\\","
            + "\\\"parentTaskUniqueCode\\\":\\\"id-2d88-1608982098027-91558\\\"}],\\\"tasks\\\":[{\\\"root\\\":false,"
            + "\\\"taskName\\\":\\\"逻辑回归二分类\\\",\\\"taskUniqueCode\\\":\\\"id-9531-1608891073887-61143\\\"},{\\\"root\\\":false,"
            + "\\\"taskName\\\":\\\"随机森林\\\",\\\"taskUniqueCode\\\":\\\"id-3d25-1608980864737-77856\\\"},{\\\"root\\\":false,"
            + "\\\"taskName\\\":\\\"类型转换\\\",\\\"taskUniqueCode\\\":\\\"id-2d88-1608982098027-91558\\\"},{\\\"root\\\":false,"
            + "\\\"taskName\\\":\\\"数据视图\\\",\\\"taskUniqueCode\\\":\\\"id-2317-1608984201281-74996\\\"},{\\\"root\\\":false,"
            + "\\\"taskName\\\":\\\"拆分\\\",\\\"taskUniqueCode\\\":\\\"id-85ae-1608984392467-81946\\\"},{\\\"root\\\":false,"
            + "\\\"taskName\\\":\\\"预测\\\",\\\"taskUniqueCode\\\":\\\"id-4191-1608984751235-85036\\\"},{\\\"root\\\":false,"
            + "\\\"taskName\\\":\\\"预测\\\",\\\"taskUniqueCode\\\":\\\"id-6b26-1608984755520-07203\\\"},{\\\"root\\\":false,"
            + "\\\"taskName\\\":\\\"二分类评估\\\",\\\"taskUniqueCode\\\":\\\"id-5dd4-1608984770722-54098\\\"},{\\\"root\\\":false,"
            + "\\\"taskName\\\":\\\"二分类评估\\\",\\\"taskUniqueCode\\\":\\\"id-41c6-1608984773170-26269\\\"},{\\\"root\\\":false,"
            + "\\\"taskName\\\":\\\"归一化\\\",\\\"taskUniqueCode\\\":\\\"id-9869-1608986331830-78910\\\"},{\\\"root\\\":true,"
            + "\\\"taskName\\\":\\\"读数据源\\\",\\\"taskUniqueCode\\\":\\\"id-9309-1608992715826-47326\\\"},{\\\"root\\\":false,"
            + "\\\"taskName\\\":\\\"SQL组件\\\",\\\"taskUniqueCode\\\":\\\"id-73af-1608994414936-19117\\\"},{\\\"root\\\":false,"
            + "\\\"taskName\\\":\\\"直方图\\\",\\\"taskUniqueCode\\\":\\\"id-3906-1608996028711-50772\\\"}],\\\"workspaceId\\\":\\\"609609\\\"}\","
            + "\"extraContent\":\"{}\"}";
        CodeModel<DefaultJsonFormCode> codeModel = CodeModelFactory.getCodeModel(CodeProgramType.PAI_STUDIO.name(), code);
        log.info("content: {}", codeModel.getCodeModel().getContent());
        log.info("extraContent: {}", codeModel.getCodeModel().getExtraContent());
        JSONObject extraContentJs = JSON.parseObject(codeModel.getCodeModel().getExtraContent());
        extraContentJs.fluentPut("experimentId", "123123").fluentPut("name", "test");
        codeModel.getCodeModel().setExtraContent(extraContentJs.toString());
        log.info("extraContent: {}", codeModel.getCodeModel().getRawContent());
        Assert.assertTrue(StringUtils.indexOf(codeModel.getCodeModel().getRawContent(), "experimentId") > 0);
    }
}
