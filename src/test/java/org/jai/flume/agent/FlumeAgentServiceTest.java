package org.jai.flume.agent;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.agent.embedded.EmbeddedAgent;
import org.jai.hbase.HbaseService;
import org.jai.search.analytics.GenerateSearchAnalyticsDataService;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class FlumeAgentServiceTest extends
		AbstractSearchJUnit4SpringContextTests {
	@Autowired
	private GenerateSearchAnalyticsDataService generateSearchAnalyticsDataService;

	@Autowired
	private FlumeAgentService flumeAgentService;
	@Autowired
	private HbaseService hbaseService;

	@Test
	public void testGetFlumeAgent() throws EventDeliveryException,
			InterruptedException {

		EmbeddedAgent flumeAgent = flumeAgentService.getFlumeAgent();

		int searchEventsCount = 11;
		List<Event> searchEvents = generateSearchAnalyticsDataService
				.getSearchEvents(searchEventsCount);
		for (Event event : searchEvents) {
			flumeAgent.put(event);
		}
		// wait until sink process everything. sleep 10 sec.
		Thread.sleep(10000);
	}
	
	@Test
	public void testProcessEvents() throws EventDeliveryException,
			InterruptedException {

		int searchEventsCount = 100;
		generateSearchAnalyticsDataService
				.generateAndPushSearchEvents(searchEventsCount);;
		
		// wait until sink process everything. sleep 10 sec.
		Thread.sleep(10000);
		assertEquals(searchEventsCount, hbaseService.getTotalSearchClicksCount());
	}
}
