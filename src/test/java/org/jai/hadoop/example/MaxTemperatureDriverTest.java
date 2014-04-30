package org.jai.hadoop.example;

import java.util.UUID;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class MaxTemperatureDriverTest
{
    @Test
//    @Ignore
    public void test() throws Exception
    {
        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        int exitCode = ToolRunner.run(driver, new String[]{"sample.txt", "target/hdfsouput-" + UUID.randomUUID()});
//        System.exit(exitCode);
        System.out.println("Exit code is:" + exitCode);
    }
}
