package com.aliyun.dataworks.migrationx.transformer.dataworks.apps;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.aliyun.dataworks.client.command.CommandApp;

import com.google.common.base.Joiner;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataWorksAirflowV2TransformerApp extends CommandApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataWorksAirflowV2TransformerApp.class);

    private static final String OPT_DAG_FOLDER = "d";
    private static final String OPT_DAG_FOLDER_LONG = "dag_folder";
    private static final String OPT_OUTPUT_FOLDER = "o";
    private static final String OPT_OUTPUT_FOLDER_LONG = "output";
    private static final String OPT_TYPE_MAPPING = "m";
    private static final String OPT_TYPE_MAPPING_LONG = "mapping";
    private static final String OPT_PATH_PREFIX = "p";
    private static final String OPT_PATH_PREFIX_LONG = "prefix";
    private static final String OPT_HELP = "h";
    private static final String OPT_HELP_LONG = "help";

    public static void main(String[] args) throws Exception {
        DataWorksAirflowV2TransformerApp app = new DataWorksAirflowV2TransformerApp();
        app.run(args);
    }

    private void runCommand(File workingDir, String... command) throws IOException {
        LOGGER.info("run command: {}", Joiner.on(" ").join(command));
        try {
            ProcessBuilder pb = new ProcessBuilder().directory(workingDir).command(command);
            new File(workingDir, "stdout.log");
            new File(workingDir, "stderr.log");
            pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            pb.redirectError(pb.redirectOutput());
            Process pro = pb.start();
            pro.waitFor();
            if (pro.exitValue() == 0) {
                LOGGER.info("run command completed");
            } else {
                LOGGER.error("run command failed");
            }
        } catch (InterruptedException e) {
            LOGGER.error("run command interrupted: ", e);
        }
    }

    @Override
    public void run(String[] args) throws Exception {
        try {
            Options options = new Options();
            options.addOption(OPT_DAG_FOLDER, OPT_DAG_FOLDER_LONG, true, "airflow dag folder");
            options.addOption(OPT_OUTPUT_FOLDER, OPT_OUTPUT_FOLDER_LONG, true, "workflow storing folder");
            options.addOption(OPT_TYPE_MAPPING, OPT_TYPE_MAPPING_LONG, true, "type mapping json file");
            options.addOption(OPT_PATH_PREFIX, OPT_PATH_PREFIX_LONG, true, "workflow location prefix in IDE");
            options.addOption(OPT_HELP, OPT_HELP_LONG, false, "show help.");

            CommandLineParser parser = new DefaultParser();
            CommandLine cli = parser.parse(options, args);
            HelpFormatter helpFormatter = new HelpFormatter();
            if (cli.hasOption(OPT_HELP)) {
                helpFormatter.printHelp("Options", options);
                System.exit(0);
            }

            if (!cli.hasOption(OPT_DAG_FOLDER)) {
                helpFormatter.printHelp("Options", options);
                LOGGER.error("Option needed: {}", options.getOption(OPT_DAG_FOLDER).toString());
                System.exit(-1);
            }
            if (!cli.hasOption(OPT_OUTPUT_FOLDER)) {
                helpFormatter.printHelp("Options", options);
                LOGGER.error("Option needed: {}", options.getOption(OPT_OUTPUT_FOLDER).toString());
                System.exit(-1);
            }

            File workingDir = new File(System.getProperty("currentDir"));
            File bin = new File(workingDir, Joiner.on(File.separator).join(
                    "lib", "python", "airflow-workflow", "parser.py"
            ));

            List<String> pythonArgs = new ArrayList<>();
            pythonArgs.add("python");
            pythonArgs.add(bin.getAbsolutePath());
            pythonArgs.addAll(Arrays.asList(args));
            LOGGER.info("args: {}", Arrays.asList(args));
            runCommand(workingDir, pythonArgs.toArray(new String[0]));
        } catch (ParseException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
