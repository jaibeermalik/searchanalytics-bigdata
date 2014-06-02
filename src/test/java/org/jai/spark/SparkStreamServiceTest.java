package org.jai.spark;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.agent.embedded.EmbeddedAgent;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.junit.After;
import org.junit.Before;
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
		generateSearchAnalyticsDataService
				.generateAndPushSearchEvents(searchEventsCount);
		sparkStreamService.setup();
	}

	@After
	public void afterDone() {
		sparkStreamService.shutdown();
	}

	@Test
//	@Ignore
	public void startHDFSTxtFileStreams() throws InterruptedException,
			IOException {
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
		sparkStreamService.startFlumeStream();
		EmbeddedAgent agent;
		final Map<String, String> properties = new HashMap<String, String>();
		properties.put("channel.type", "memory");
		properties.put("channel.capacity", "100000");
		properties.put("channel.transactionCapacity", "1000");
		properties.put("sinks", "sink1");
		properties.put("sink1.type", "avro");
		properties.put("sink1.hostname", "localhost");
		properties.put("sink1.port", "41111");
		properties.put("processor.type", "default");
		agent = new EmbeddedAgent("sparkstreamingagent");
		agent.configure(properties);
		agent.start();

		for (int i = 0; i < 5; i++) {
			List<Event> searchEvents = generateSearchAnalyticsDataService
					.getSearchEvents(searchEventsCount);
			agent.putAll(searchEvents);
			// After every 1 sec send more data
			Thread.sleep(1000);
		}
		Thread.sleep(10000);
		agent.stop();
	}
}
