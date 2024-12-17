/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.common;

import java.util.ArrayList;
import java.util.Objects;
import java.util.function.Function;

import com.aliyun.dataworks.common.spec.domain.SpecRefEntity;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.Priority;
import com.aliyun.dataworks.migrationx.transformer.flowspec.converter.dolphinscheduler.common.context.DolphinSchedulerV3ConverterContext;
import com.aliyun.dataworks.migrationx.transformer.flowspec.model.SpecRefEntityWrapper;
import com.aliyun.migrationx.common.utils.UuidUtils;
import org.apache.commons.lang3.BooleanUtils;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-07-04
 */

public abstract class AbstractCommonConverter<T> {

    protected final DolphinSchedulerV3ConverterContext context;

    private final Function<Long, String> generateUuidFunc;

    protected AbstractCommonConverter(DolphinSchedulerV3ConverterContext context) {
        this.context = context;
        if (BooleanUtils.isTrue(context.getDirectMappingId())) {
            generateUuidFunc = String::valueOf;
        } else {
            generateUuidFunc = code -> UuidUtils.genUuidWithoutHorizontalLine();
        }
    }

    /**
     * convert to T type object
     *
     * @return T type object
     */
    protected abstract T convert();

    /**
     * convert dolphin priority to spec priority
     *
     * @param priority dolphin priority
     * @return spec priority
     */
    protected Integer convertPriority(Priority priority) {
        return Priority.LOWEST.getCode() - priority.getCode();
    }

    /**
     * generate uuid and save mapping between code and uuid in context.
     * it will be used in build dependencies
     *
     * @param code dolphinScheduler entity code
     * @return uuid
     */
    protected String generateUuid(Long code) {
        if (Objects.isNull(code)) {
            return generateUuid();
        }
        context.getCodeUuidMap().computeIfAbsent(code, generateUuidFunc);
        return context.getCodeUuidMap().get(code);
    }

    /**
     * generate uuid and save mapping between code and uuid in context.
     * it will be used in build dependencies
     *
     * @param code   dolphinScheduler entity code
     * @param entity flow spec entity
     * @return uuid
     */
    protected String generateUuid(Long code, SpecRefEntity entity) {
        String uuid = generateUuid(code);
        if (Objects.nonNull(entity)) {
            entity.setId(uuid);
            context.getSpecRefEntityMap().put(uuid, newWrapper(entity));
        }
        return uuid;
    }

    /**
     * generate uuid and save in context
     *
     * @return uuid
     */
    protected String generateUuid(SpecRefEntity specRefEntity) {
        return generateUuid(null, specRefEntity);
    }

    protected String generateUuid() {
        String uuid = UuidUtils.genUuidWithoutHorizontalLine();
        context.getCodeUuidMap().putIfAbsent(Long.valueOf(uuid), uuid);
        return uuid;
    }

    protected SpecFlowDepend newSpecFlowDepend() {
        SpecFlowDepend specFlowDepend = new SpecFlowDepend();
        specFlowDepend.setDepends(new ArrayList<>());
        return specFlowDepend;
    }

    protected SpecRefEntityWrapper newWrapper(SpecRefEntity entity) {
        return new SpecRefEntityWrapper().setSpecRefEntity(entity);
    }
}
