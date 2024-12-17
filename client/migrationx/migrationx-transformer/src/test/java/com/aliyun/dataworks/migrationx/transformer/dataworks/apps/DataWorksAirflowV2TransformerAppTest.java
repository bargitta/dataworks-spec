package com.aliyun.dataworks.migrationx.transformer.dataworks.apps;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.fail;
@Ignore
public class DataWorksAirflowV2TransformerAppTest {
    @Before
    public void setupTest() {
        System.setProperty("currentDir", ".");
    }

    @Test
    public void testRunApp() {
        DataWorksAirflowV2TransformerApp transformerApp = new DataWorksAirflowV2TransformerApp();
        String[] args = new String[]{
                "-d", "airflow2/dags/",
                "-o", "airflow2/workflows/",
                "-m", "airflow2/config/mappings.json"
        };
        try {
            transformerApp.run(args);
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("No exception should have been thrown out, but got this: " + ex.getClass());
        }
        try {
            DataWorksAirflowV2TransformerApp.main(args);
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("No exception should have been thrown out, but got this: " + ex.getClass());
        }
    }
}
