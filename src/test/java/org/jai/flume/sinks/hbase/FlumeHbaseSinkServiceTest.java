package org.jai.flume.sinks.hbase;

import java.util.List;

import org.apache.flume.Event;
import org.jai.hbase.HbaseService;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class FlumeHbaseSinkServiceTest extends AbstractSearchJUnit4SpringContextTests{

	@Autowired
	private FlumeHbaseSinkService flumeHbaseSinkService;
	@Autowired
	private HbaseService hbaseService;
	
	@Test
	public void testProcessEvents() {
		hbaseService.removeAll();
		int searchEventsCount = 400;
		List<Event> searchEvents = generateSearchAnalyticsDataService
				.getSearchEvents(searchEventsCount);
		flumeHbaseSinkService.processEvents(searchEvents);
//		assertEquals(searchEventsCount, hbaseService.getTotalSearchClicksCount());
		
//		hbaseService.getSearchClicks();
//		hbaseService.findTotalRecordsForValidCustomers();
//		hbaseService.findTopTenSearchQueryStringForLastAnHour();
	}

}
