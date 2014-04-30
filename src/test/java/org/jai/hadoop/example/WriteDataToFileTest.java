package org.jai.hadoop.example;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.jai.flume.sinks.elasticsearch.FlumeESSinkService;
import org.jai.flume.sinks.hdfs.FlumeHDFSSinkService;
import org.jai.hadoop.hdfs.HadoopClusterService;
import org.jai.hive.HiveSearchClicksService;
import org.jai.search.analytics.GenerateSearchAnalyticsDataService;
import org.jai.search.client.SearchClientService;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class WriteDataToFileTest extends AbstractSearchJUnit4SpringContextTests {

	@Autowired
	private GenerateSearchAnalyticsDataService generateSearchAnalyticsDataService;
	@Autowired
	private SearchClientService searchClientService;
	@Autowired
	private HadoopClusterService hadoopClusterService;
	@Autowired
	private FlumeESSinkService flumeESSinkService;
	@Autowired
	private FlumeHDFSSinkService flumeHDFSSinkService;
	@Autowired
	private HiveSearchClicksService hiveSearchClicksService;

	@Test
	public void test() {
		try {

			// Generate Events
			System.out.println("Running test now!" + hadoopClusterService.getHDFSUri());
			
			List<Event> searchEvents = generateSearchAnalyticsDataService
					.getSearchEvents(10);
			// Write events to file and read.
//			hdfsFileLoggerSinkAndTest(searchEvents);

			// Write events to hdfs sink and test data.
			FlumehdfsSinkAndTestData(searchEvents);

			FlumeESSinkAndTestData(searchEvents);
			
			TestHiveDatabase();

		} catch (EventDeliveryException | InterruptedException | IOException e) {
			e.printStackTrace();
			fail();
		}
	}

	private void TestHiveDatabase() {
		hiveSearchClicksService.setup();
		for (String dbString : hiveSearchClicksService.getDbs()) {
			System.out.println("Db name is:" + dbString);
			for (String TbString : hiveSearchClicksService.getTables(dbString)) {
				System.out.println("Table name is:" + TbString);
			}
		}
	}

	private void FlumeESSinkAndTestData(List<Event> searchEvents)
			throws EventDeliveryException, IOException, FileNotFoundException {
		flumeESSinkService.processEvents(searchEvents);

		Client client = searchClientService.getClient();
		client.admin().indices().refresh(Requests.refreshRequest()).actionGet();

		String indexName = "recentlyviewed" + '-'
				+ ElasticSearchIndexRequestBuilderFactory.df.format(new Date());
		long totalCount = client.prepareCount(indexName).get().getCount();
		System.out.println("Search total count is: " + totalCount);

		SearchHits hits = client.prepareSearch(indexName).get().getHits();
		System.out.println("Total hits: " + hits.getTotalHits());
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSource());
		}
	}

	private void FlumehdfsSinkAndTestData(List<Event> searchEvents)
			throws EventDeliveryException, IOException, FileNotFoundException {

		flumeHDFSSinkService.processEvents(searchEvents);

		// list all files and check data.
		Path dirPath = new Path(hadoopClusterService.getHDFSUri() + "/searchevents");
		// FileStatus[] dirStat = fs.listStatus(dirPath);
		// Path fList[] = FileUtil.stat2Paths(dirStat);

		DistributedFileSystem fs = hadoopClusterService.getFileSystem();
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(dirPath, true);
		while (files.hasNext()) {
			LocatedFileStatus locatedFileStatus = files.next();
			System.out.println("Check:" + locatedFileStatus.getPath());
			if (locatedFileStatus.isFile()) {
				Path path = locatedFileStatus.getPath();
				if (path.getName().startsWith("searchevents")) {
					FSDataInputStream input = fs.open(path);
					BufferedReader reader = new BufferedReader(
							new InputStreamReader(input));
					String body = null;
					while ((body = reader.readLine()) != null) {
						System.out.println("body is:" + body);
					}
					reader.close();
				}
			}
		}
	}

	private void hdfsFileLoggerSinkAndTest(List<Event> searchEvents)
			throws FileNotFoundException, IOException {

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
