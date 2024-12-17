package com.aliyun.dataworks.migrationx.writer.dolphinscheduler;

import java.io.File;
import java.net.URI;

import com.aliyun.dataworks.client.command.CommandApp;
import com.aliyun.migrationx.common.http.HttpClientUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-10-08
 */
public class DolphinSchedulerSingleJsonWriter extends CommandApp {

    private static final String IMPORT_API = "/dolphinscheduler/projects/%s/process-definition/import";

    private static final String HEADER_TOKEN = "token";
    private static final Logger log = LoggerFactory.getLogger(DolphinSchedulerSingleJsonWriter.class);

    public static void main(String[] args) throws Exception {
        new DolphinSchedulerSingleJsonWriter().run(args);
    }

    @Override
    public void run(String[] args) throws Exception {
        Options options = getOptions();
        CommandLine commandLine = getCommandLine(options, args);

        String endpoint = commandLine.getOptionValue("e");
        String token = commandLine.getOptionValue("t");
        String dolphinProjectCode = commandLine.getOptionValue("p");
        String filePath = commandLine.getOptionValue("f");

        File file = new File(filePath);
        if (!file.exists()) {
            log.error("file not exists, file: {}", filePath);
            throw new IllegalArgumentException("file not exists, file: " + filePath);
        }
        write(endpoint, dolphinProjectCode, token, file);
    }

    protected Options getOptions() {
        Options options = new Options();
        options.addRequiredOption("e", "endpoint", true, "dolphinscheduler endpoint, example: http://123.23.23.34:12345");
        options.addRequiredOption("t", "token", true, "dolphinscheduler token");
        options.addRequiredOption("p", "projectCode", true, "dolphinscheduler project code");
        options.addRequiredOption("f", "sourceFile", true, "dolphinscheduler json source path");
        return options;
    }

    private void write(String endpoint, String projectCode, String token, File file) {
        HttpClientUtil client = new HttpClientUtil();
        try {
            HttpPost httpPost = new HttpPost();
            httpPost.setHeader(HEADER_TOKEN, token);
            String finalUrl = endpoint + String.format(IMPORT_API, projectCode);
            httpPost.setURI(new URI(finalUrl));

            MultipartEntityBuilder builder = MultipartEntityBuilder.create();
            builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
            builder.addBinaryBody("file", file);
            HttpEntity entity = builder.build();
            httpPost.setEntity(entity);

            HttpResponse httpResponse = client.executeAndGetHttpResponse(httpPost);
            log.info("write process to project response: {}", httpResponse);
            if (httpResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                log.error("write process to project failed, endpoint: {}, projectCode: {}", endpoint, projectCode);
            }
        } catch (Exception e) {
            log.error("write process to project error, endpoint: {}, projectCode: {}", endpoint, projectCode, e);
        }
    }
}
