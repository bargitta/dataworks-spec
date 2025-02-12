package com.aliyun.migrationx.common.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import lombok.Data;

@Data
public class Config {
    private static ThreadLocal<Config> THREAD_LOCAL = new ThreadLocal<>();

    public static Config get() {
        return THREAD_LOCAL.get();
    }

    public static void init(Config config) {
        THREAD_LOCAL.set(config);
        if (config.getReplaceMapping() != null) {
            for (Replaced replaced : config.getReplaceMapping()) {
                replaced.setParsedPattern(Pattern.compile(replaced.getPattern()));
            }
        }
    }

    public static void init() {
        THREAD_LOCAL.set(new Config());
    }

    public boolean isVersion32() {
        return version.startsWith("3.2");
    }
    /**
     * transformer
     */
    private boolean skipUnSupportType = false;

    private boolean transformContinueWithError = false;

    /**
     * spec continue when error with some spec
     */
    private boolean specContinueWithError = false;

    private List<String> skipTypes = new ArrayList<>();

    private List<String> skipTaskCodes = new ArrayList<>();
    private List<String> tempTaskTypes = new ArrayList<>();

    private boolean zipSpec = true;

    /**
     * filter transform task by list
     *
     * {projectName}.{processName}.{taskName}
     * '*' for all
     */
    private List<String> filterTasks = new ArrayList<>();

    /**
     * filter by process
     */
    private ProcessFilter processFilter;

    /**
     * workflow base path
     */
    private String basePath;

    private List<Replaced> replaceMapping;

    private String source;

    private String version;

    private boolean includeGlobalParam = true;

    @Data
    public static class Replaced {
        private String taskType;
        private String desc;
        private String pattern;
        private Pattern parsedPattern;
        private String target;
        private String param;
    }

    @Data
    public static class ProcessFilter {
        private String releaseState;
        private boolean includeSubProcess;
    }
}
