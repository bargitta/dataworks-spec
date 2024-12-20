import com.aliyun.dataworks.migrationx.domain.adf.AdfPackage;
import com.aliyun.dataworks.migrationx.domain.adf.AdfPackageLoader;
import com.aliyun.dataworks.migrationx.domain.adf.Trigger;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Map;

public class AdfPackageLoaderTest {

    @Test
    public void testLoadPackage_withTriggersOnly() {
        File packageFile = new File("src/test/resources/");
        AdfPackageLoader loader = new AdfPackageLoader(packageFile);
        AdfPackage adfPackage = loader.loadPackage();
        Assert.assertTrue(adfPackage.getPipelines().isEmpty());
        Assert.assertTrue(adfPackage.getLinkedServices().isEmpty());
        Map<String,Trigger> triggers = adfPackage.getTriggers();
        Assert.assertEquals(2, triggers.size());

        Assert.assertTrue(triggers.containsKey("pipeline_department1_1"));
        Assert.assertTrue(triggers.containsKey("parallel_activity"));
    }
}
