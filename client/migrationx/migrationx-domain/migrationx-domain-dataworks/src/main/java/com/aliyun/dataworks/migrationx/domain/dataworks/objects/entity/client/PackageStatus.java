package com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-11-29
 */
public enum PackageStatus {

    TERMINATION(-1),

    INIT(0),

    RUNNING(1),

    SUCCESS(2),

    FAIL(3),
    CANCEL(4);
    private final Integer code;

    PackageStatus(Integer code) {
        this.code = code;
    }

    public Integer getCode() {
        return this.code;
    }

    public static PackageStatus getByCode(Integer code) {
        for (PackageStatus status : PackageStatus.values()) {
            if (code.equals(status.getCode())) {
                return status;
            }
        }
        return null;
    }
}

