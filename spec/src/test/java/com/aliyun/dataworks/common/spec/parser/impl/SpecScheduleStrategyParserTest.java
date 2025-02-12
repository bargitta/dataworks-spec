package com.aliyun.dataworks.common.spec.parser.impl;

import java.util.HashMap;
import java.util.Map;

import com.aliyun.dataworks.common.spec.domain.enums.FailureStrategy;
import com.aliyun.dataworks.common.spec.domain.enums.NodeInstanceModeType;
import com.aliyun.dataworks.common.spec.domain.enums.NodeRecurrenceType;
import com.aliyun.dataworks.common.spec.domain.enums.NodeRerunModeType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScheduleStrategy;
import com.aliyun.dataworks.common.spec.parser.SpecParserContext;
import org.junit.Assert;
import org.junit.Test;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2025-01-21
 */
public class SpecScheduleStrategyParserTest {

    private final SpecScheduleStrategyParser specScheduleStrategyParser = new SpecScheduleStrategyParser();

    @Test
    public void testParser() {
        SpecScheduleStrategy param = new SpecScheduleStrategy();
        param.setFailureStrategy(FailureStrategy.BREAK);
        param.setRerunInterval(18000);
        param.setRerunMode(NodeRerunModeType.FAILURE_ALLOWED);
        param.setRerunTimes(3);
        param.setRecurrenceType(NodeRecurrenceType.NORMAL);
        param.setInstanceMode(NodeInstanceModeType.T_PLUS_1);
        param.setTimeout(18000);
        param.setPriority(1);
        param.setIgnoreBranchConditionSkip(true);

        Map<String, Object> map = new HashMap<>();
        map.put("failureStrategy", FailureStrategy.BREAK.getLabel());
        map.put("rerunInterval", 18000);
        map.put("rerunMode", NodeRerunModeType.FAILURE_ALLOWED.getLabel());
        map.put("rerunTimes", 3);
        map.put("recurrenceType", NodeRecurrenceType.NORMAL.getLabel());
        map.put("instanceMode", NodeInstanceModeType.T_PLUS_1.getLabel());
        map.put("timeout", 18000);
        map.put("priority", 1);
        map.put("ignoreBranchConditionSkip", true);

        SpecScheduleStrategy result = specScheduleStrategyParser.parse(map, new SpecParserContext());
        Assert.assertEquals(param, result);
    }
}
