package com.aliyun.dataworks.migrationx.domain.adf;

import lombok.Data;

@Data
public class Identity {
    private String type;
    private String principalId;
    private String tenantId;
}
