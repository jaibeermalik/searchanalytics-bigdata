package org.jai.spark;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.flume.EventDeliveryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class SparkStreamServiceTest extends
		AbstractSearchJUnit4SpringContextTests {

	@Autowired
	private SparkStreamService sparkStreamService;
	private int searchEventsCount = 200;

	@Before
	public void prepareHdfs() {
		DistributedFileSystem fs = hadoopClusterService.getFileSystem();
		Path path = new Path("/searchevents");
		Path sparkPath = new Path("/sparkcheckpoint");
		try {
			fs.delete(path, true);
			fs.delete(sparkPath, true);
			fs.mkdirs(path);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	@Ignore
	public void startHDFSTxtFileStreams() throws InterruptedException,
			IOException {
		sparkStreamService.shutdown();
		sparkStreamService.startHDFSTxtFileStreams();
		for (int i = 0; i < 5; i++) {
			generateSearchAnalyticsDataService
					.generateAndPushSearchEvents(searchEventsCount);
			// After every 1 sec send more data
			Thread.sleep(1000);
		}
		// printHdfsData();
		Thread.sleep(10000);
	}

	@Test
	public void startFlumeStream() throws InterruptedException, IOException,
			EventDeliveryException {
		for (int i = 0; i < 5; i++) {
			generateSearchAnalyticsDataService
					.generateAndPushSearchEvents(searchEventsCount);
			// After every 1 sec send more data
			Thread.sleep(1000);
		}
	}
}
