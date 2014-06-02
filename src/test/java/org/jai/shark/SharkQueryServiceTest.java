package org.jai.shark;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.jai.hive.HiveSearchClicksService;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.jai.spark.SparkStreamService;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class SharkQueryServiceTest extends AbstractSearchJUnit4SpringContextTests{

	@Autowired
	private SharkQueryService sharkQueryService;
	
	@Autowired
	private SparkStreamService sparkStreamService;
	@Autowired
	private HiveSearchClicksService hiveSearchClicksService;
	private int searchEventsCount = 200;
	private String year;
	private String month;
	private String day;
	private String hour;
	private String dbName = "search";
	private String tbName = "search_clicks";
	
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
	@Ignore
	//TODO: issue with hive version, need to fix that first.
	public void test() {
		sharkQueryService.getSearchClicks(dbName, tbName);
	}

}
