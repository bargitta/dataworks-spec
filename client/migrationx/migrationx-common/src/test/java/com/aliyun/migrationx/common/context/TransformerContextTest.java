package com.aliyun.migrationx.common.context;

import com.aliyun.migrationx.common.metrics.enums.CollectorType;

import org.junit.Assert;
import org.junit.Test;

public class TransformerContextTest {

    @Test
    public void testTransformerContextCustomResourceDir() {
        TransformerContext.init(CollectorType.DolphinScheduler);
        TransformerContext.getContext().setCustomResourceDir(".");
        Assert.assertEquals(".", TransformerContext.getContext().getCustomResourceDir().getName());
    }
}