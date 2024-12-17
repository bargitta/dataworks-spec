package com.aliyun.dataworks.migrationx.writer.dataworks.model;

import com.aliyun.dataworks.common.spec.domain.interfaces.LabelEnum;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-10-17
 */
@Getter
@AllArgsConstructor
public enum AsyncJobStatus implements LabelEnum {

    /**
     * 运行中
     */
    RUNNING("Running", false),

    /**
     * 成功
     */
    SUCCESS("Success", true),

    /**
     * 失败
     */
    FAIL("Fail", true),

    /**
     * 取消
     */
    CANCEL("Cancel", true),
    ;
    private final String label;

    private final Boolean completed;
}
