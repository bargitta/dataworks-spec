package com.aliyun.dataworks.migrationx.reader.dolphinscheduler;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class DolphinSchedulerCommandAppTest {
    @Test
    public void testReader1x() {
        DolphinSchedulerCommandApp app = new DolphinSchedulerCommandApp();
        String[] args = new String[]{
                "-e", "http://8.152.5.70:12345/",
                "-t", "37fc0fd51131cdba974879d1bf9x0da78",
                "-v", "1.3.5",
                "-p", "test1234",
                "-f", "../../temp/13"
        };
        app.run(args);
    }

    @Test
    public void testReader2x() {
        DolphinSchedulerCommandApp app = new DolphinSchedulerCommandApp();
        String[] args = new String[]{
                "-e", "http://101.200.34.168:12345",
                "-t", "dbdab854de9695fb3bd2efax30f59d8d0",
                "-v", "2.0.5",
                "-p", "proj1",
                "-f", "temp/13666515015680"
        };
        app.run(args);
    }

    @Test
    public void testReader3x() {
        DolphinSchedulerCommandApp app = new DolphinSchedulerCommandApp();
        String[] args = new String[]{
                "-e", "http://39.106.89.151:12345/",
                "-t", "xx",
                "-v", "3.1.5",
                "-p", "code:15688846682752",
                "-f", "temp/max2"
        };
        app.run(args);
    }

    @Test
    public void testReader3x2() {
        DolphinSchedulerCommandApp app = new DolphinSchedulerCommandApp();
        String[] args = new String[]{
                "-e", "http://39.106.89.151:12345/",
                "-t", "xxxxx",
                "-v", "3.1.5",
                "-p", "p_sparks",
                "-f", "temp/max2"
        };
        app.run(args);
    }
}