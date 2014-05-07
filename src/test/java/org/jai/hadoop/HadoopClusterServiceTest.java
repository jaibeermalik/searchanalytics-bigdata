package org.jai.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.UUID;

import org.apache.flume.Event;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.jai.search.analytics.GenerateSearchAnalyticsDataService;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class HadoopClusterServiceTest extends
		AbstractSearchJUnit4SpringContextTests {

	@Autowired
	private HadoopClusterService hadoopClusterService;
	@Autowired
	private GenerateSearchAnalyticsDataService generateSearchAnalyticsDataService;

	@Test
	public void testGetFileSystem() {
		assertNotNull(hadoopClusterService.getFileSystem());
	}

	@Test
	public void testGetHDFSUri() {
		assertEquals("hdfs://localhost.localdomain:54321",
				hadoopClusterService.getHDFSUri());
	}

	@Test
	public void testGetJobTRackerUri() {
		assertEquals("localhost.localdomain:54310",
				hadoopClusterService.getJobTRackerUri());
	}

	@Test
	public void hdfsFileLoggerSinkAndTest() throws FileNotFoundException,
			IOException {

		List<Event> searchEvents = generateSearchAnalyticsDataService
				.getSearchEvents(11);

		DistributedFileSystem fs = hadoopClusterService.getFileSystem();

		// /Write to file
		Path outFile = new Path("/searchevents/event" + UUID.randomUUID());
		FSDataOutputStream out = fs.create(outFile, false);
		for (Event event : searchEvents) {
			String eventString = new String(event.getBody(), "UTF-8");
			System.out.println("Writing event string: " + eventString);
			out.writeUTF(eventString + System.lineSeparator());
		}
		out.flush();
		out.close();

		// check the data is there...with standard file
		FSDataInputStream input = fs.open(outFile);
		try (BufferedReader br = new BufferedReader(new InputStreamReader(
				input, "UTF-8"))) {
			String line = null;
			while ((line = br.readLine()) != null) {
				System.out.println("HDFS file line is:" + line);
			}
		}

		input.close();
		fs.delete(outFile, true);
	}

}
