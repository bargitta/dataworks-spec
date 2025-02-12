/*
 * Copyright (c) 2024, Alibaba Cloud;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.dataworks.migrationx.reader.dolphinscheduler;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.dataworks.client.command.CommandApp;
import com.aliyun.migrationx.common.utils.JSONUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DolphinScheduler Exporter
 */
@Slf4j
public class DolphinSchedulerCommandApp extends CommandApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(DolphinSchedulerCommandApp.class);
    private static final String LONG_OPT_SKIP_RESOURCES = "skip-resources";
    private static final String OPT_SKIP_RESOURCES = "sr";
    private static final String CODE_LABEL = "code";
    private static final String NAME_LABEL = "name";

    @Override
    public void run(String[] args) {
        Options options = getOptions();
        String javaVersion = System.getProperty("java.version");
        log.info("running with java version: {}", javaVersion);
        log.info("args: {}", JSONUtils.toJsonString(args));
        HelpFormatter helpFormatter = new HelpFormatter();
        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine commandLine = parser.parse(options, args);
            String endpoint = commandLine.getOptionValue("e");
            String token = commandLine.getOptionValue("t");
            String version = commandLine.getOptionValue("v");
            String projectStr = commandLine.getOptionValue("p");
            Pair<List<String>, List<Long>> pair = parseProjects(projectStr);
            String file = commandLine.getOptionValue("f", "output");

            DolphinSchedulerReader exporter = new DolphinSchedulerReader(
                    endpoint, token, version, pair.getLeft(), pair.getRight(),
                    new File(new File(file).getAbsolutePath()));
            String sr = commandLine.getOptionValue(OPT_SKIP_RESOURCES, "true");
            exporter.setSkipResources(Boolean.parseBoolean(sr));
            File exportedFile = exporter.export();
            LOGGER.info("exported file: {}", exportedFile);
        } catch (ParseException e) {
            LOGGER.error("parser command error: {}", e.getMessage());
            helpFormatter.printHelp("Options", options);
            System.exit(-1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addRequiredOption("e", "endpoint", true,
                "DolphinScheduler Web API endpoint url, example: http://123.123.123.12:12343");
        options.addRequiredOption("t", "token", true, "DolphinScheduler API token");
        options.addRequiredOption("v", "version", true, "DolphinScheduler version");
        options.addOption("p", "projects", true, "DolphinScheduler project names, example: project_a,project_b,project_c");
        options.addOption("f", "file", true, "Output zip file");
        options.addOption(OPT_SKIP_RESOURCES, LONG_OPT_SKIP_RESOURCES, true, "skip exporting resources");
        return options;
    }

    private Pair<List<String>, List<Long>> parseProjects(String projectStr) {
        List<String> projects = new ArrayList<>();
        List<Long> codes = new ArrayList<>();
        String[] projectTypes = projectStr.split(":");
        if (projectTypes.length == 2) {
            if (CODE_LABEL.equalsIgnoreCase(projectTypes[0])) {
                String[] codeStr = projectTypes[1].split(",");
                codes = Arrays.stream(codeStr).map(Long::parseLong).collect(Collectors.toList());
            } else {
                String[] codeStr = projectTypes[1].split(",");
                projects = Arrays.asList(codeStr);
            }
        } else {
            projects = Arrays.asList(StringUtils.split(projectStr, ","));
        }
        return Pair.of(projects, codes);
    }

    public static void main(String[] args) {
        DolphinSchedulerCommandApp app = new DolphinSchedulerCommandApp();
        app.run(args);
    }
}
