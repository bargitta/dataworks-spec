package com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client;

import lombok.Getter;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-11-29
 */
@Getter
public enum DeployDetailStatus {

    UNPUBLISHED(0, "UNPUBLISHED"),
    SUCCESS(1, "SUCCESS"),
    ERROR(2, "ERROR"),
    CLONED(3, "CLONED"),
    DEPLOY_ERROR(4, "DEPLOY_ERROR"),
    CLONING(5, "CLONING"),
    REJECT(6, "REJECT"), //发布驳回
    ;

    private Integer code;

    private String alias;

    private DeployDetailStatus(Integer code, String alias) {
        this.code = code;
        this.alias = alias;
    }

    public Integer getCode() {
        return this.code;
    }

    public String getAlias() {
        return this.alias;
    }

    /**
     * 通过code来获取枚举对象
     *
     * @param code
     * @return
     */
    public static DeployDetailStatus getByCode(Integer code) {
        for (DeployDetailStatus type : DeployDetailStatus.values()) {
            if (code.equals(type.getCode())) {
                return type;
            }
        }
        return null;
    }

    public static DeployDetailStatus getByAlias(String alias) {
        for (DeployDetailStatus type : DeployDetailStatus.values()) {
            if (alias.equals(type.getAlias())) {
                return type;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return code.toString();
    }
}
