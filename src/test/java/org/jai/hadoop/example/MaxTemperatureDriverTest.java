package org.jai.hadoop.example;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Ignore;
import org.junit.Test;

public class MaxTemperatureDriverTest
{
    @Test
//    @Ignore
    public void test() throws Exception
    {
        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        int exitCode = ToolRunner.run(driver, new String[]{"sample.txt", "hdfsouput"});
        System.exit(exitCode);
    }
}
