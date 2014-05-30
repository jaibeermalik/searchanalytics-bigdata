package org.jai.spark;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class SparkStreamServiceTest extends AbstractSearchJUnit4SpringContextTests{

	@Autowired
	private SparkStreamService sparkStreamService;
	private int searchEventsCount = 200;
	
	@Before
	public void prepareHdfs() {
		DistributedFileSystem fs = hadoopClusterService.getFileSystem();
		Path path = new Path("/searchevents");
		try {
			fs.delete(path, true);
			fs.mkdirs(path);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		generateSearchAnalyticsDataService
		.generateAndPushSearchEvents(searchEventsCount);
		sparkStreamService.startHDFSTxtFileStreams();
	}

	@Test
	public void startHDFSTxtFileStreams() throws InterruptedException, IOException {
		for(int i=0; i<10; i++)
		{
			generateSearchAnalyticsDataService
			.generateAndPushSearchEvents(searchEventsCount);
			//After every 1 sec send more data
			Thread.sleep(1000);
		}
//		printHdfsData();
		Thread.sleep(10000);
		sparkStreamService.shutdown();
	}

}
