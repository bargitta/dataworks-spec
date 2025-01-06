package com.aliyun.dataworks.migrationx.reader.adf;

import com.aliyun.dataworks.client.command.CommandApp;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;

import java.io.File;

@Slf4j
public class AdfCommandApp extends CommandApp {

    @Override
    public void run(String[] args) throws Exception {
        Options options = new Options();
        options.addRequiredOption("t", "token", true, "DataFactory Token");
        options.addOption("o", "output", true, "Output zip file");
        options.addRequiredOption("s", "subscriptionId", true, "azure subscription id");
        options.addOption("f", "factory", true, "factory name");
        options.addOption("r", "resourceGroupName", true, "data factory resource group name");

        HelpFormatter helpFormatter = new HelpFormatter();
        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine commandLine = parser.parse(options, args);
            String token = commandLine.getOptionValue("t");
            String subscriptionId = commandLine.getOptionValue("s");
            String resourceGroupName = commandLine.getOptionValue("r");
            String factory = commandLine.getOptionValue("f");
            String output = commandLine.getOptionValue("o", "output.zip");
            AdfReader exporter = new AdfReader(token, subscriptionId, resourceGroupName, factory,
                    new File(new File(output).getAbsolutePath()));
            File exportedFile = exporter.export();
            log.info("exported file: {}", exportedFile);
        } catch (ParseException e) {
            log.error("parser command error: {}", e.getMessage());
            helpFormatter.printHelp("Options", options);
            System.exit(-1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
