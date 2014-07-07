package org.jai.hbase;

import org.apache.flume.Event;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;

public class HbaseServiceTest extends AbstractSearchJUnit4SpringContextTests {

	@Autowired
	private HbaseService hbaseService;
	private int searchEventsCount = 200;

	@Test
	public void testHbaseServer() {
		hbaseService.testHbaseServer();
	}

	@Test
	public void testSearchClicksEventsData() {
		hbaseService.removeAll();
		for (Event event : generateSearchAnalyticsDataService
				.getSearchEvents(searchEventsCount)) {
			hbaseService.insertEventData(event.getBody());
		}
		assertEquals(searchEventsCount,
				hbaseService.getTotalSearchClicksCount());
	}

	@Test
	public void testSearchClicksEventsDataForFlumeAgent() throws InterruptedException {
		hbaseService.removeAll();
		generateSearchAnalyticsDataService
				.generateAndPushSearchEvents(searchEventsCount);
		//wait 10 sec for the hbase data to get processed
		Thread.sleep(10000);
		assertEquals(searchEventsCount,
				hbaseService.getTotalSearchClicksCount());
	}
	
	@Test
	public void findTotalRecordsForValidCustomers() throws InterruptedException {
		hbaseService.removeAll();
		generateSearchAnalyticsDataService
				.generateAndPushSearchEvents(searchEventsCount);
		//wait 10 sec for the hbase data to get processed
		Thread.sleep(1000);
		assertTrue(hbaseService.getTotalSearchClicksCount() > 0);
	}
	
	@Test
	public void findTopTenSearchQueryStringForLastAnHour() throws InterruptedException {
		hbaseService.removeAll();
		generateSearchAnalyticsDataService
				.generateAndPushSearchEvents(1000);
		//wait 10 sec for the hbase data to get processed
		Thread.sleep(1000);
		assertTrue(hbaseService.findTopTenSearchQueryStringForLastAnHour().size() > 0);
	}

	@Test
	public void findTopTenSearchFiltersForLastAnHour() throws InterruptedException {
		hbaseService.removeAll();
		generateSearchAnalyticsDataService
				.generateAndPushSearchEvents(1000);
		//wait 10 sec for the hbase data to get processed
		Thread.sleep(1000);
		assertTrue(hbaseService.findTopTenSearchFiltersForLastAnHour().size() > 0);
	}

}
