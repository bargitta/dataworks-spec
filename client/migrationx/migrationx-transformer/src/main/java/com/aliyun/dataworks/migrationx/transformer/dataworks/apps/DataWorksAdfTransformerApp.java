package com.aliyun.dataworks.migrationx.transformer.dataworks.apps;

import com.aliyun.dataworks.migrationx.domain.adf.AdfPackage;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DataWorksPackage;
import com.aliyun.dataworks.migrationx.domain.dataworks.standard.objects.Package;
import com.aliyun.dataworks.migrationx.transformer.core.BaseTransformerApp;
import com.aliyun.dataworks.migrationx.transformer.core.transformer.Transformer;
import com.aliyun.dataworks.migrationx.transformer.dataworks.transformer.DataWorksAdfTransformer;
import com.aliyun.migrationx.common.context.TransformerContext;
import com.aliyun.migrationx.common.metrics.enums.CollectorType;

import java.io.File;

public class DataWorksAdfTransformerApp extends BaseTransformerApp {
    public DataWorksAdfTransformerApp() {
        super(AdfPackage.class, DataWorksPackage.class);
    }

    @Override
    public void initCollector() {
        TransformerContext.init(CollectorType.AzureDataFactory);
        super.initCollector();
    }

    @Override
    protected Transformer createTransformer(File config, Package from, Package to) {
        AdfPackage dataFactoryPackage = (AdfPackage)from;
        DataWorksPackage dataWorksPackage = (DataWorksPackage)to;
        return new DataWorksAdfTransformer(config, dataFactoryPackage, dataWorksPackage);
    }
}
