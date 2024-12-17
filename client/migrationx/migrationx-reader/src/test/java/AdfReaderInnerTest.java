import com.aliyun.dataworks.migrationx.reader.adf.AdfReader;
import com.google.gson.JsonObject;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.List;

@Ignore
public class AdfReaderInnerTest {
    private static final String token = "get token from https://learn.microsoft.com/en-us/rest/api/datafactory/pipelines/list-by-factory?view=rest-datafactory-2018-06-01&tabs=HTTP&tryIt=true&source=docs#code-try-0";
    private static final String factory = "chenxi-df";

    private static final String resrouceGroupName = "datafactory-rg923";
    private static final String id = "097597a0-5749-49fa-968c-e556a3ea8a76";


    @Test
    public void test_export() throws Exception {
        File file = new File("/Users/xichen/Documents/adf");
        AdfReader reader = new AdfReader(token, id, resrouceGroupName, factory, file, null);
        List<JsonObject> triggers = reader.listTriggers();
        System.out.println(triggers);
        List<JsonObject> pipelines = reader.listPipelines();
        System.out.println(pipelines);
        List<JsonObject> services = reader.listLinkedServices();
        System.out.println(services);
    }
}
