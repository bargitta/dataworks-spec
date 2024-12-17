package com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.v320.task.condition;

import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.parameters.AbstractParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.v320.task.Dependence;
import lombok.Data;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-10-23
 */
@Data
public class ConditionsParameters extends AbstractParameters {

    private Dependence dependence;

    private ConditionResult conditionResult;

    @Override
    public boolean checkParameters() {
        return true;
    }
}
