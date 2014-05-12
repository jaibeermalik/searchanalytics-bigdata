package org.jai.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class HiveSearchClicksServiceTest extends
		AbstractSearchJUnit4SpringContextTests {

	private int searchEventsCount = 200;
	private String year;
	private String month;
	private String day;
	private String hour;
	private String dbName = "search";
	private String tbName = "search_clicks";

	@Autowired
	private HiveSearchClicksService hiveSearchClicksService;

	@Before
	public void prepareHive() {
		DistributedFileSystem fs = hadoopClusterService.getFileSystem();
		Path path = new Path("/searchevents");
		try {
			fs.delete(path, true);
			fs.mkdirs(path);
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		hiveSearchClicksService.setup();
	}

	private void prepareHiveData() {
		generateSearchAnalyticsDataService
		.generateAndPushSearchEvents(searchEventsCount);
		addPartitions();
	}

	@Test
	public void testGetDbs() {
		for (String dbString : hiveSearchClicksService.getDbs()) {
			System.out.println("Db name is:" + dbString);
		}
	}

	@Test
	public void testGetTables() {
		for (String dbString : hiveSearchClicksService.getDbs()) {
			for (String TbString : hiveSearchClicksService.getTables(dbString)) {
				System.out.println("Table name is:" + TbString);
			}
		}
	}

	@Test
	public void testGetTotalSearchClicksCount() throws FileNotFoundException,
			IOException {
		prepareHiveData();
		int searchClicksCount = hiveSearchClicksService.getTotalRowCount(
				dbName, tbName);
		System.out.println("Search clicks count is:" + searchClicksCount);
		assertEquals(searchEventsCount, searchClicksCount);
	}

	@Test
	public void testAddPartition() {
		addPartitions();
	}

	private void addPartitions() {
		// Add partition
		DateTime now = new DateTime();
		int monthOfYear = now.getMonthOfYear();
		int dayOfMonth = now.getDayOfMonth();
		int hourOfDay = now.getHourOfDay();
		year = String.valueOf(now.getYear());
		month = monthOfYear < 10 ? "0" + String.valueOf(monthOfYear) : String
				.valueOf(monthOfYear);
		day = dayOfMonth < 10 ? "0" + String.valueOf(dayOfMonth) : String
				.valueOf(dayOfMonth);
		hour = hourOfDay < 10 ? "0" + String.valueOf(hourOfDay) : String
				.valueOf(hourOfDay);
		// String tbNameInternal = "search_clicks_internal";

		hiveSearchClicksService.addPartition(dbName, tbName, year, month, day,
				hour);
		// hiveSearchClicksService.addPartition(dbName, tbNameInternal, year,
		// month, day, hour);
	}

	@Test
	public void testGetSearchClicks() {
		prepareHiveData();
		hiveSearchClicksService.getSearchClicks(dbName, tbName, year, month,
				day, hour);
		// hiveSearchClicksService.getSearchClicks(dbName, tbNameInternal, year,
		// month, day, hour);
	}

	@Test
	public void testLoadSearchCustomerQueryTable() {
		prepareHiveData();
		hiveSearchClicksService.loadSearchCustomerQueryTable();
		int totalRowCount = hiveSearchClicksService.getTotalRowCount(dbName,
				"search_customerquery");
		System.out.println("Search search_customerquery count is:"
				+ totalRowCount);
		assertTrue(totalRowCount > 0);
	}

	@Test
	public void testLoadTopSearchCustomerQueryToElasticSearchIndex() {
		prepareHiveData();
		hiveSearchClicksService.loadSearchCustomerQueryTable();
		hiveSearchClicksService
				.loadTopSearchCustomerQueryToElasticSearchIndex();

		int totalRowCount = hiveSearchClicksService.getTotalRowCount(dbName,
				"search_customerquery");
		System.out.println("Search search_customerquery_to_es count is:"
				+ totalRowCount);
		assertTrue(totalRowCount > 0);

		String indexName = "topqueries";
		Client client = searchClientService.getClient();
		boolean exists = client.admin().indices().prepareExists(indexName)
				.get().isExists();
		long totalCount = 0;
		if (exists) {
			totalCount = client.prepareCount(indexName).get().getCount();
			for (SearchHit searchHit : client.prepareSearch(indexName)
					.setSize(100).get().getHits()) {
				System.out.println(searchHit.getSource());
			}
		}
		System.out.println("Total topqueries count:" + totalCount);
	}

}
