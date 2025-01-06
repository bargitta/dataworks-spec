package com.aliyun.migrationx.common.context;

import java.io.File;
import java.io.FileNotFoundException;

import com.aliyun.migrationx.common.metrics.DefaultMetricCollector;
import com.aliyun.migrationx.common.metrics.DolphinMetricsCollector;
import com.aliyun.migrationx.common.metrics.MetricsCollector;
import com.aliyun.migrationx.common.metrics.enums.CollectorType;

import org.apache.commons.lang3.StringUtils;

public class TransformerContext {
    private static final ThreadLocal<TransformerContext> threadLocal = ThreadLocal.withInitial(() -> {
        TransformerContext ctx = new TransformerContext();
        ctx.metricsCollector = new DefaultMetricCollector();
        return ctx;
    });

    private MetricsCollector metricsCollector;

    private File checkpoint;
    private File load;

    private File sourceDir;

    private File customResourceDir;

    private String scriptDir;

    public String getScriptDir() {
        return scriptDir;
    }

    public void setScriptDir(String scriptDir) {
        this.scriptDir = scriptDir;
    }

    public File getCheckpoint() {
        return checkpoint;
    }

    public void setCheckpoint(String checkpoint) throws FileNotFoundException {
        if (StringUtils.isNotEmpty(checkpoint)) {
            File file = new File(checkpoint);
            if (file.exists()) {
                this.checkpoint = file;
            } else {
                throw new FileNotFoundException(checkpoint);
            }
        }
    }

    public File getLoad() {
        return load;
    }

    public void setLoad(String load) throws FileNotFoundException {
        if (StringUtils.isNotEmpty(load)) {
            File file = new File(load);
            if (file.exists()) {
                this.load = file;
            } else {
                throw new FileNotFoundException(load);
            }
        }
    }

    private TransformerContext() {
    }

    public static void init(CollectorType type) {
        TransformerContext context = new TransformerContext();
        switch (type) {
            case DolphinScheduler:
                context.metricsCollector = new DolphinMetricsCollector();
                break;
            default:
                context.metricsCollector = new DefaultMetricCollector();
        }
        threadLocal.set(context);
    }

    public static TransformerContext getContext() {
        return threadLocal.get();
    }

    public static MetricsCollector getCollector() {
        return threadLocal.get().metricsCollector;
    }

    public static void clear() {
        threadLocal.remove();
    }

    public File getSourceDir() {
        return sourceDir;
    }

    public void setSourceDir(File sourceDir) {
        this.sourceDir = sourceDir;
    }

    public File getCustomResourceDir() {
        return customResourceDir;
    }

    public void setCustomResourceDir(String resourceDir) {
        if (StringUtils.isEmpty(resourceDir)) {
            return;
        }
        File file = new File(resourceDir);
        if (!file.exists()) {
            throw new RuntimeException(String.format("resource dir %s with parameter rs not exists", resourceDir));
        }
        this.customResourceDir = file;
    }
}
