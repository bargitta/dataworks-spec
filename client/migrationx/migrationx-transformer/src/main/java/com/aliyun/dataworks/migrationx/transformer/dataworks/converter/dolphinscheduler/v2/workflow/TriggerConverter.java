/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v2.workflow;

import java.text.ParseException;
import java.time.Duration;
import java.util.Objects;

import com.aliyun.dataworks.common.spec.domain.enums.NodeRecurrenceType;
import com.aliyun.dataworks.common.spec.domain.enums.TriggerType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecTrigger;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.Schedule;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.Flag;
import com.aliyun.dataworks.migrationx.domain.dataworks.utils.CronExpressUtil;
import com.aliyun.migrationx.common.utils.DateUtils;
import com.aliyun.migrationx.common.utils.UuidGenerators;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TriggerConverter {

    private final Schedule schedule;

    private final SpecTrigger currTrigger;

    private final TaskDefinition taskDefinition;

    private SpecTrigger convertRes;

    public TriggerConverter(Schedule schedule) {
        super();
        this.schedule = schedule;
        this.currTrigger = null;
        this.taskDefinition = null;
        this.convertRes = new SpecTrigger();
    }

    public TriggerConverter(SpecTrigger trigger, TaskDefinition taskDefinition) {
        super();
        this.schedule = null;
        this.currTrigger = trigger;
        this.taskDefinition = taskDefinition;
        this.convertRes = new SpecTrigger();
    }

    public SpecTrigger convert() {
        if (Objects.nonNull(schedule)) {
            convertBySchedule();
        } else if (Objects.nonNull(currTrigger) && Objects.nonNull(taskDefinition)) {
            convertByTaskDefinition();
        } else {
            return null;
        }
        return convertRes;
    }

    /**
     * convert workflow trigger
     */
    private void convertBySchedule() {
        if (Objects.isNull(schedule)) {
            convertRes.setType(TriggerType.MANUAL);
        } else {
            convertRes.setId(UuidGenerators.generateUuid(Long.valueOf(schedule.getId())));
            convertRes.setType(TriggerType.SCHEDULER);
            convertRes.setStartTime(DateUtils.convertDateToString(schedule.getStartTime()));
            convertRes.setEndTime(DateUtils.convertDateToString(schedule.getEndTime()));
            convertRes.setCron(dolphinCron2SpecCron(schedule.getCrontab()));
            convertRes.setTimezone(schedule.getTimezoneId());
            convertRes.setDelaySeconds(0);
        }
    }

    /**
     * convert task trigger, especially for delay seconds.
     * Because node will use same trigger with parent workflow except delay seconds.
     */
    private void convertByTaskDefinition() {
        if (currTrigger == null || taskDefinition == null) {
            throw new RuntimeException("currTrigger or taskDefinition null");
        }
        //convertRes = BeanUtils.deepCopy(currTrigger, SpecTrigger.class);
        convertRes = currTrigger;
        convertRes.setDelaySeconds((int) Duration.ofMinutes(taskDefinition.getDelayTime()).getSeconds());
        convertRes.setRecurrence(Flag.YES.equals(taskDefinition.getFlag()) ? NodeRecurrenceType.NORMAL : NodeRecurrenceType.PAUSE);
        convertRes.setId(UuidGenerators.generateUuid());
    }

    private String dolphinCron2SpecCron(String dolphinCron) {
        try {
            return CronExpressUtil.quartzCronExpressionToDwCronExpress(dolphinCron);
        } catch (ParseException e) {
            log.warn("dolphin cron parse error: {}", dolphinCron);
            return dolphinCron;
        }
    }
}