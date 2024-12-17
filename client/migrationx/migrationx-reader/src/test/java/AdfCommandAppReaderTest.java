import com.aliyun.dataworks.migrationx.reader.adf.AdfCommandApp;
import org.junit.Ignore;
import org.junit.Test;
@Ignore
public class AdfCommandAppReaderTest {

    /**
     * azure data factory reader test
     * output files are pipelines.json, triggers.json, and linked_services.json
     * @throws Exception exception when calling api/write files
     */
    @Test
    public void test_export() throws Exception {
        AdfCommandApp app = new AdfCommandApp();
        String[] args = new String[]{
                "-t", "get token from https://learn.microsoft.com/en-us/rest/api/datafactory/pipelines/list-by-factory?view=rest-datafactory-2018-06-01&tabs=HTTP&tryIt=true&source=docs#code-try-0",
                "-f", "chenxi-df",
                "-s", "097597a0-5749-49fa-968c-e556a3ea8a76",
                "-r", "datafactory-rg923",
                "-o", "../../temp/adf"
        };
        app.run(args);
    }
}
