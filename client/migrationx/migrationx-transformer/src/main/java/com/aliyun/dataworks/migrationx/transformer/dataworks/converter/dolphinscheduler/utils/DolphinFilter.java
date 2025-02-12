package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.utils;

import java.util.List;

import com.aliyun.migrationx.common.utils.Config;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DolphinFilter {
    public static boolean willConvert(String processName, String releaseState, List<String> codes) {
        Config.ProcessFilter filter = Config.get().getProcessFilter();
        if (filter != null) {
            String state = filter.getReleaseState();
            if (state != null) {
                if (releaseState != null && releaseState.equals(state)) {
                    return true;
                } else {
                    boolean includeSubProcess = filter.isIncludeSubProcess();
                    if (includeSubProcess && isInSubProcess(codes)) {
                        log.info("filterProcess: {} state: {} in subprocess", processName, releaseState);
                        return true;
                    }

                    log.warn("filterProcess: {} state: {} is not equals {}, skip", processName, releaseState, state);
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean isInSubProcess(List<String> codes) {
        if (codes != null) {
            return true;
        } else {
            return false;
        }
    }
}
