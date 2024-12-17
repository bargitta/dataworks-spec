package com.aliyun.dataworks.migrationx.transformer.dataworks.transformer;

import com.aliyun.dataworks.common.spec.SpecUtil;
import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.enums.SpecVersion;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.utils.JSONUtils;
import com.aliyun.dataworks.migrationx.domain.adf.AdfConf;
import com.aliyun.dataworks.migrationx.domain.adf.AdfPackage;
import com.aliyun.dataworks.migrationx.domain.adf.AdfPackageFileService;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DataWorksPackage;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DwProject;
import com.aliyun.dataworks.migrationx.transformer.core.transformer.AbstractPackageTransformer;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.adf.AdfConverter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Slf4j
public class DataWorksAdfTransformer extends AbstractPackageTransformer<AdfPackage, DataWorksPackage> {

    private AdfConf adfConf;

    private List<Specification<DataWorksWorkflowSpec>> specs;

    public DataWorksAdfTransformer(File configFile, AdfPackage sourcePackage, DataWorksPackage targetPackage) {
        super(configFile, sourcePackage, targetPackage);
        this.sourcePackageFileService = new AdfPackageFileService();
        adfConf = loadConf(configFile);
    }

    public AdfConf loadConf(File configFile) {
        try {
            String config = FileUtils.readFileToString(configFile, StandardCharsets.UTF_8);
            this.adfConf = JSONUtils.parseObject(config, new TypeReference<AdfConf>() {
            });
            return adfConf;
        } catch (IOException e) {
            log.warn("config file not exists: {}", configFile);
        }
        return AdfConf.DEFAULT;
    }

    @Override
    public void init() throws Exception {
    }

    @Override
    public void load() throws Exception {
        this.sourcePackageFileService.load(sourcePackage);
    }

    @Override
    public void transform() throws Exception {
        AdfConverter converter = new AdfConverter(sourcePackageFileService.getPackage(), adfConf);
        List<SpecWorkflow> workflows = converter.convert();
        specs = new ArrayList<>(workflows.size());
        for (SpecWorkflow w : workflows) {
            Specification<DataWorksWorkflowSpec> workflowSpecFile = toWorkflowSpecFile(w);
            specs.add(workflowSpecFile);
        }
    }
    private Specification<DataWorksWorkflowSpec> toWorkflowSpecFile(SpecWorkflow w) {
        DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
        spec.setWorkflows(ImmutableList.of(w));
        spec.setName(w.getName());
        Specification<DataWorksWorkflowSpec> sp = new Specification<>();
        sp.setKind(SpecKind.CYCLE_WORKFLOW.getLabel());
        sp.setVersion(SpecVersion.V_1_1_0.getLabel());
        sp.setSpec(spec);
        return sp;
    }

    @Override
    public void write() throws Exception {

        File packageFile = targetPackage.getPackageFile();
        if(!packageFile.exists()){
            FileUtils.forceMkdir(packageFile);
        }
        File targetTmp = new File(packageFile, ".tmp");
        if (targetTmp.exists()) {
            FileUtils.deleteDirectory(targetTmp);
        }
        FileUtils.forceMkdir(targetTmp);
        specs.forEach(spec->{
            String fileName = spec.getSpec().getName();

            String pathname = targetTmp + "/" + fileName + ".json";
            File file = new File(pathname);
            try {
                FileUtils.writeStringToFile(file, SpecUtil.writeToSpec(spec), StandardCharsets.UTF_8);
            } catch (IOException e) {
                log.error("failed to write file");
            }
        });
    }
}
