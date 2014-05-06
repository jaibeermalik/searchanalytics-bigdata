package org.jai.flume.sinks.elasticsearch;

import java.util.Date;
import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.jai.search.analytics.GenerateSearchAnalyticsDataService;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class FlumeESSinkServiceTest extends AbstractSearchJUnit4SpringContextTests{

	@Autowired
	private GenerateSearchAnalyticsDataService generateSearchAnalyticsDataService;
	@Autowired
	private FlumeESSinkService flumeESSinkService;
	
	@Test
	public void testProcessEvents() {
		int searchEventsCount = 101;
		List<Event> searchEvents = generateSearchAnalyticsDataService
				.getSearchEvents(searchEventsCount);
		
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

}
