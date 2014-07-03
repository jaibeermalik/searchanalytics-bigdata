package org.jai.setup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.jai.elasticsearch.ElasticSearchRepoService;
import org.jai.flume.sinks.elasticsearch.FlumeESSinkService;
import org.jai.flume.sinks.hbase.FlumeHbaseSinkService;
import org.jai.flume.sinks.hdfs.FlumeHDFSSinkService;
import org.jai.hadoop.HadoopClusterService;
import org.jai.hbase.HbaseService;
import org.jai.hive.HiveSearchClicksService;
import org.jai.oozie.OozieJobsService;
import org.jai.search.analytics.GenerateSearchAnalyticsDataService;
import org.jai.search.client.SearchClientService;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.joda.time.DateTime;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class CompleteSetupIntegrationTest extends
		AbstractSearchJUnit4SpringContextTests {

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
	private FlumeHbaseSinkService flumeHbaseSinkService;
	@Autowired
	private HiveSearchClicksService hiveSearchClicksService;
	@Autowired
	private OozieJobsService oozieJobsService;
	@Autowired
	private ElasticSearchRepoService customerTopQueryService;
	@Autowired
	private HbaseService hbaseService;

	private int searchEventsCount = 10;

	@Test
	public void testFullDataFlow() {
		try {

			// Generate Events

			hadoopClusterService.getFileSystem().delete(
					new Path("/searchevents"), true);

			List<Event> searchEvents = generateSearchAnalyticsDataService
					.getSearchEvents(searchEventsCount);

			// Write events to hdfs sink and test data.
			FlumehdfsSinkAndTestData(searchEvents);

			FlumeESSinkAndTestData(searchEvents);

			FlumeHbaseSinkAndTestData(searchEvents);

			TestHiveDatabaseSearchClicks();

			startOozieAddHivePartitionJob();
			// sleep 10 sec for partition to be added.
			// Thread.sleep(100*1000);

			testTopCustomerQueriesWithHiveAndES();

		} catch (EventDeliveryException | InterruptedException | IOException e) {
			e.printStackTrace();
			fail();
		}
	}

	private void startOozieAddHivePartitionJob() {
		// oozieJobsService.startHiveAddPartitionCoordJob();
	}

	private void TestHiveDatabaseSearchClicks() throws InterruptedException {
		// Setup
		hiveSearchClicksService.setup();
		for (String dbString : hiveSearchClicksService.getDbs()) {
			System.out.println("Db name is:" + dbString);
			for (String TbString : hiveSearchClicksService.getTables(dbString)) {
				System.out.println("Table name is:" + TbString);
			}
		}

		// Add partition
		DateTime now = new DateTime();
		int monthOfYear = now.getMonthOfYear();
		int dayOfMonth = now.getDayOfMonth();
		int hourOfDay = now.getHourOfDay();
		String year = String.valueOf(now.getYear());
		String month = monthOfYear < 10 ? "0" + String.valueOf(monthOfYear)
				: String.valueOf(monthOfYear);
		String day = dayOfMonth < 10 ? "0" + String.valueOf(dayOfMonth)
				: String.valueOf(dayOfMonth);
		String hour = hourOfDay < 10 ? "0" + String.valueOf(hourOfDay) : String
				.valueOf(hourOfDay);
		String dbName = "search";
		String tbName = "search_clicks";
		// String tbNameInternal = "search_clicks_internal";

		hiveSearchClicksService.addPartition(dbName, tbName, year, month, day,
				hour);
		// hiveSearchClicksService.addPartition(dbName, tbNameInternal, year,
		// month, day, hour);
		// Thread.sleep(1000);
		// Query Data
		// TODO: count equal data: searchEventsCount
		// int searchClicksCount = hiveSearchClicksService
		// .getTotalSearchClicksCount(dbName, tbName);
		// System.out.println("Search clicks count is:" + searchClicksCount);

		hiveSearchClicksService.getSearchClicks(dbName, tbName, year, month,
				day, hour);
		// hiveSearchClicksService.getSearchClicks(dbName, tbNameInternal, year,
		// month, day, hour);

		// Thread.sleep(10000000);
	}

	private void testTopCustomerQueriesWithHiveAndES() {
		hiveSearchClicksService.loadSearchCustomerQueryTable();

		hiveSearchClicksService
				.loadTopSearchCustomerQueryToElasticSearchIndex();

		String indexName = "topqueries";
		Client client = searchClientService.getClient();
		boolean exists = client.admin().indices().prepareExists(indexName)
				.get().isExists();
		long totalCount = 0;
		if (exists) {
			totalCount = client.prepareCount(indexName).get().getCount();
		}
		System.out.println("Total topqueries count:" + totalCount);

		for (SearchHit searchHit : client.prepareSearch(indexName)
				.setSize(searchEventsCount).get().getHits()) {
			System.out.println(searchHit.getSource());
		}

		long countTotalRecords = customerTopQueryService
				.countCustomerTopQueryTotalRecords();
		assertTrue(countTotalRecords > 0);
		assertEquals(totalCount, countTotalRecords);
		customerTopQueryService.deleteAllCustomerTopQueryRecords();
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
		Path dirPath = new Path(hadoopClusterService.getHDFSUri()
				+ "/searchevents");
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
					input.close();
				}
			}
		}
	}

	private void FlumeHbaseSinkAndTestData(List<Event> searchEvents)
			throws EventDeliveryException, IOException, FileNotFoundException {
		hbaseService.removeAll();
		flumeHbaseSinkService.processEvents(searchEvents);
		
		assertEquals(searchEventsCount, hbaseService.getTotalSearchClicksCount());
	}

}
