package com.aliyun.dataworks.migrationx.reader.adf;

import com.aliyun.migrationx.common.http.HttpClientUtil;
import com.aliyun.migrationx.common.utils.GsonUtils;
import com.aliyun.migrationx.common.utils.ZipUtils;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpGet;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import static com.aliyun.dataworks.migrationx.reader.adf.AdfCommandApp.GLOBAL_HOST;

@Slf4j
public class AdfReader {

    private static final String PIPELINE = "pipelines";

    private static final String TRIGGER = "triggers";

    private static final String LINKED_SERVICE = "linked_services";

    private static final String JSON_SUFFIX = ".json";
    public static final String NEXT_LINK = "nextLink";

    private final String subscriptionId;
    private final String resourceGroupName;
    private final String token;
    private final String factory;
    private final File exportFile;

    private final String host;

    public AdfReader(String token, String subscriptionId, String resourceGroupName, String factory, File exportFile, String host) {
        this.subscriptionId = subscriptionId;
        this.resourceGroupName = resourceGroupName;
        this.factory = factory;
        this.token = token;
        this.exportFile = exportFile;
        if (StringUtils.isBlank(host)) {
            this.host = GLOBAL_HOST;
        } else {
            this.host = host;
        }
    }

    public File export() throws Exception {
        File parent = new File(exportFile.getParentFile(), StringUtils.split(exportFile.getName(), ".")[0]);

        if (!parent.exists() && !parent.mkdirs()) {
            log.error("failed create file directory for: {}", exportFile);
            return null;
        }

        log.info("workspace directory: {}", parent);

        File tmpDir = new File(parent, ".tmp");
        if (tmpDir.exists()) {
            FileUtils.deleteDirectory(tmpDir);
        }

        doExport(tmpDir);
        if (exportFile.getName().endsWith("zip")) {
            return doPackage(tmpDir, exportFile);
        } else {
            return tmpDir;
        }
    }

    private void doExport(File tmpDir) throws Exception {
        exportPipelines(tmpDir);
        exportTriggers(tmpDir);
        exportLinkedServices(tmpDir);
    }

    public File doPackage(File tmpDir, File exportFile) throws IOException {
        return ZipUtils.zipDir(tmpDir, exportFile);
    }

    private void exportLinkedServices(File factoryDir) throws Exception {
        File curFactory = new File(factoryDir, this.factory);
        List<JsonObject> linkedServices = listResources(MessageFormat.format(
                this.host + "/subscriptions/{0}/resourceGroups/{1}/providers/Microsoft"
                        + ".DataFactory/factories/{2}/linkedservices?api-version=2018-06-01",
                subscriptionId,
                resourceGroupName, factory));
        if (CollectionUtils.isNotEmpty(linkedServices)) {
            FileUtils.writeStringToFile(new File(curFactory, LINKED_SERVICE + JSON_SUFFIX), GsonUtils.toJsonString(linkedServices),
                    StandardCharsets.UTF_8);
        }
    }

    public void exportPipelines(File factoryDir) throws Exception {
        File curFactory = new File(factoryDir, this.factory);
        String url = MessageFormat.format(
                this.host + "/subscriptions/{0}/resourceGroups/{1}/providers/Microsoft"
                        + ".DataFactory/factories/{2}/pipelines?api-version=2018-06-01", this.subscriptionId,
                this.resourceGroupName, this.factory);
        List<JsonObject> pipelines = listResources(url);
        if (CollectionUtils.isNotEmpty(pipelines)) {
            FileUtils.writeStringToFile(new File(curFactory, PIPELINE + JSON_SUFFIX), GsonUtils.toJsonString(pipelines), StandardCharsets.UTF_8);
        }
    }

    public void exportTriggers(File factoryDir) throws Exception {
        File curFactory = new File(factoryDir, this.factory);
        List<JsonObject> triggers = listResources(MessageFormat.format(
                this.host + "/subscriptions/{0}/resourceGroups/{1}/providers/Microsoft.DataFactory" +
                        "/factories/{2}/triggers?api-version=2018-06-01",
                this.subscriptionId,
                this.resourceGroupName, this.factory));
        if (CollectionUtils.isNotEmpty(triggers)) {
            FileUtils.writeStringToFile(new File(curFactory, TRIGGER + JSON_SUFFIX), GsonUtils.toJsonString(triggers), StandardCharsets.UTF_8);
        }
    }

    public List<JsonObject> listResources(String url) throws Exception {
        List<JsonObject> pipelines = new ArrayList<>();
        do {
            JsonObject jsonObject = GsonUtils.fromJsonString(executeGet(url, token), new TypeToken<JsonObject>() {
            }.getType());
            JsonArray jsonArray = jsonObject.get("value").getAsJsonArray();
            pipelines.addAll(GsonUtils.gson.fromJson(jsonArray, new TypeToken<List<JsonObject>>() {
            }.getType()));
            if (jsonObject.get(NEXT_LINK) != null) {
                url = jsonObject.get(NEXT_LINK).getAsString();
            } else {
                url = null;
            }
        } while (url != null);
        return pipelines;
    }

    private static String executeGet(String url, String token) throws Exception {
        HttpClientUtil client = new HttpClientUtil();
        HttpGet httpGet = new HttpGet();
        httpGet.setHeader("Authorization", "Bearer " + token);
        httpGet.setURI(new URI(url));
        return client.executeAndGet(httpGet);
    }
}
