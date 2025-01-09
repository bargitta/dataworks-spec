package com.aliyun.dataworks.migrationx.writer;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import com.aliyun.dataworks_public20240518.Client;
import com.aliyun.dataworks_public20240518.models.GetJobStatusResponse;
import com.aliyun.dataworks_public20240518.models.GetJobStatusResponseBody;
import com.aliyun.dataworks_public20240518.models.GetJobStatusResponseBody.GetJobStatusResponseBodyJobStatus;
import com.aliyun.dataworks_public20240518.models.ImportWorkflowDefinitionResponse;
import com.aliyun.dataworks_public20240518.models.ImportWorkflowDefinitionResponseBody;
import com.aliyun.dataworks_public20240518.models.ImportWorkflowDefinitionResponseBody.ImportWorkflowDefinitionResponseBodyAsyncJob;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class DataWorksMigrationSpecificationImportWriterTest {

    @Test
    public void runApp() {
        URL url = Thread.currentThread().getContextClassLoader().getResource("airflow2/workflows/");
        DataWorksMigrationSpecificationImportWriter writer = new DataWorksMigrationSpecificationImportWriter();
        String[] args = new String[]{
                "-i", "xx",  //accessId
                "-k", "xx",  //accessKey
                "-r", "cn-shanghai",   //regionId
                "-p", "483776",   //projectId
                "-f", "../../../temp/specs_target"
        };
        try {
            writer.run(args);
        } catch (Exception e) {
            e.printStackTrace();
            fail("No exception should have been thrown out, but got this: " + e.getClass());
        }
    }

    @Test
    public void testImportSingleFlowSpec() throws Exception {
        DataWorksMigrationSpecificationImportWriter writer = new DataWorksMigrationSpecificationImportWriter();
        URL url = Thread.currentThread().getContextClassLoader().getResource("airflow2/workflows/sample.json");
        Path specFile = Paths.get(url.toURI());

        Client client = Mockito.mock(Client.class);
        ImportWorkflowDefinitionResponse resp = mock(ImportWorkflowDefinitionResponse.class);
        doReturn(ImmutableMap.of("x-acs-trace-id", "mock-acs-req-id")).when(resp).getHeaders();
        ImportWorkflowDefinitionResponseBody respBody = mock(ImportWorkflowDefinitionResponseBody.class);
        ImportWorkflowDefinitionResponseBodyAsyncJob respBodyAsyncJob = mock(ImportWorkflowDefinitionResponseBodyAsyncJob.class);
        doReturn("async-job-id").when(respBodyAsyncJob).getId();
        doReturn(respBodyAsyncJob).when(respBody).getAsyncJob();
        doReturn("mock-req-id").when(respBody).getRequestId();
        doReturn(200).when(resp).getStatusCode();
        doReturn(respBody).when(resp).getBody();
        doReturn(resp).when(client).importWorkflowDefinition(any());

        GetJobStatusResponse mockJobStatusResponse = mock(GetJobStatusResponse.class);
        GetJobStatusResponseBody mockJobStatusBody = mock(GetJobStatusResponseBody.class);
        doReturn(mockJobStatusBody).when(mockJobStatusResponse).getBody();
        doReturn(mockJobStatusResponse).when(client).getJobStatus(any());
        GetJobStatusResponseBodyJobStatus running = mock(GetJobStatusResponseBodyJobStatus.class);
        doReturn("Running").when(running).getStatus();
        GetJobStatusResponseBodyJobStatus success = mock(GetJobStatusResponseBodyJobStatus.class);
        doReturn("Success").when(success).getStatus();
        AtomicInteger i = new AtomicInteger();
        doAnswer(ivk -> {
            if (i.get() < 1) {
                i.getAndIncrement();
                return running;
            }
            return success;
        }).when(mockJobStatusBody).getJobStatus();

        writer.importSingleFlowSpec("1", client, specFile);
    }
}
