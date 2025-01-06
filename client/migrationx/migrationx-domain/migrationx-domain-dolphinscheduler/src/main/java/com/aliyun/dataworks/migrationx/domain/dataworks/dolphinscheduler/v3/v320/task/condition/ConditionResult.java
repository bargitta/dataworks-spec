package com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.v320.task.condition;

import java.util.List;

import lombok.Data;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-10-23
 */
@Data
public class ConditionResult {
    // node list to run when success
    private List<Long> successNode;

    // node list to run when failed
    private List<Long> failedNode;

}
