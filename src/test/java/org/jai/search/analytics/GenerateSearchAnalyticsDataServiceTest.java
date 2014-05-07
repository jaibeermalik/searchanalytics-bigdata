package org.jai.search.analytics;

import java.net.UnknownHostException;

import org.apache.flume.EventDeliveryException;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.core.JsonProcessingException;

public class GenerateSearchAnalyticsDataServiceTest extends
		AbstractSearchJUnit4SpringContextTests {
	@Autowired
	private GenerateSearchAnalyticsDataService generateSearchAnalyticsDataService;

	@Test
	public void generateSearchEvents() throws UnknownHostException,
			JsonProcessingException, EventDeliveryException,
			InterruptedException {
		// 1000000
		generateSearchAnalyticsDataService.generateAndPushSearchEvents(100);
	}
}
