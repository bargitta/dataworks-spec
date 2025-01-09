package com.aliyun.migrationx.common.utils;

import java.util.Objects;
import java.util.function.Function;

import static com.aliyun.migrationx.common.utils.UuidUtils.genUuidWithoutHorizontalLine;

public class UuidGenerators {

    private static final Function<Long, String> generateUuidFunc = code -> genUuidWithoutHorizontalLine();

    public static String generateUuid(Long code) {
        if (Objects.isNull(code)) {
            return generateUuid();
        }
        return generateUuidFunc.apply(code);
    }

    public static String generateUuid() {
        String uuid = genUuidWithoutHorizontalLine();
        return uuid;
    }
}
