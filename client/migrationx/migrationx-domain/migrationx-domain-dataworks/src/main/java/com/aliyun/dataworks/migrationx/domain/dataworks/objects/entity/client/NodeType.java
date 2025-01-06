package com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client;

import java.util.Arrays;
import java.util.List;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-11-22
 */
public enum NodeType {

    NORMAL(0),
    MANUAL(1),
    PAUSE(2),
    SKIP(3);

    private int code;

    private NodeType(int code) {
        this.code = code;
    }

    public int getCode() {
        return this.code;
    }

    public static NodeType getNodeTypeByCode(int code) {
        NodeType[] values = values();
        int length = values.length;

        for (int i = 0; i < length; ++i) {
            NodeType type = values[i];
            if (type.getCode() == code) {
                return type;
            }
        }

        throw new IllegalArgumentException(String.format("unknown code %d for NodeType", code));
    }

    public static List<Integer> getCalendarNodeTypeCodes() {
        return Arrays.asList(NORMAL.getCode(), PAUSE.getCode(), SKIP.getCode());
    }

}
