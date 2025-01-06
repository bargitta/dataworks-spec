package com.aliyun.dataworks.migrationx.domain.adf;

import com.aliyun.migrationx.common.utils.GsonUtils;
import com.google.gson.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class AdfPackageLoader {
    private static final String PIPELINE_JSON = "pipelines.json";
    private static final String TRIGGER_JSON = "triggers.json";
    private static final String LINKED_SERVICE_JSON = "linked_services.json";


    private final File parentFolder;

    public AdfPackageLoader(File unzippedDir) {
        this.parentFolder = unzippedDir;
    }

    public AdfPackage loadPackage() {
        AdfPackage adfPackage = new AdfPackage();
        adfPackage.setPackageRoot(parentFolder);
        adfPackage.setPipelines(getPipelines());
        adfPackage.setTriggers(getTriggers());
        adfPackage.setLinkedServices(getLinkedServices());
        return adfPackage;
    }

    public List<LinkedService> getLinkedServices() {
        try {
            File linkedServices = new File(parentFolder, LINKED_SERVICE_JSON);
            String projects = FileUtils.readFileToString(linkedServices, StandardCharsets.UTF_8);
            return GsonUtils.fromJsonString(projects, new TypeToken<List<LinkedService>>() {
            }.getType());
        }catch (IOException e) {
            log.warn("unable to load linked services", e);
        }
        return Collections.emptyList();
    }

    /**
     * this method only returns running triggers and ignores stopped ones.
     * @return map
     */
    public Map<String, Trigger> getTriggers() {
        try {
            File triggerFile = new File(parentFolder, TRIGGER_JSON);
            String projects = FileUtils.readFileToString(triggerFile, StandardCharsets.UTF_8);
            List<Trigger> triggers = GsonUtils.fromJsonString(projects, new TypeToken<List<Trigger>>() {
            }.getType());
            if (CollectionUtils.isEmpty(triggers)) {
                return Collections.emptyMap();
            }
            List<Trigger> validTriggers = triggers.stream().filter(t -> t.getProperties().getRuntimeState().equalsIgnoreCase("Started")).collect(Collectors.toList());
            Map<String, Trigger> map = new HashMap<>();
            for (Trigger trigger : validTriggers) {
                List<Trigger.LinkedPipeline> pipelines = trigger.getProperties().getPipelines();
                pipelines.forEach(p ->
                        map.put(p.pipelineReference.referenceName, trigger));
            }
            return map;

        }catch (IOException e){
            log.warn("unable to load triggers",e);
        }
        return Collections.emptyMap();
    }

    public List<Pipeline> getPipelines() {
        try {
            File pipelineFile = new File(parentFolder, PIPELINE_JSON);
            String projects = FileUtils.readFileToString(pipelineFile, StandardCharsets.UTF_8);
            return GsonUtils.fromJsonString(projects, new TypeToken<List<Pipeline>>() {
            }.getType());
        }catch (IOException e){
            log.error("unable to load pipelines", e);
        }
        return Collections.emptyList();
    }
}
