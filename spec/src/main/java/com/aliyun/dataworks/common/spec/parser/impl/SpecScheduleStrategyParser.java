package com.aliyun.dataworks.common.spec.parser.impl;

import java.util.Map;

import com.aliyun.dataworks.common.spec.annotation.SpecParser;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScheduleStrategy;
import com.aliyun.dataworks.common.spec.parser.SpecParserContext;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2025-01-21
 */
@SpecParser
public class SpecScheduleStrategyParser extends DefaultSpecParser<SpecScheduleStrategy> {
    @Override
    public SpecScheduleStrategy parse(Map<String, Object> rawContext, SpecParserContext specParserContext) {
        return super.parse(rawContext, specParserContext);
    }
}
