package org.jai.flume.sinks.hbase.serializer;

import java.net.UnknownHostException;

import org.apache.flume.Event;
import org.apache.hadoop.hbase.client.Put;
import org.jai.search.analytics.GenerateSearchAnalyticsDataService;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.core.JsonProcessingException;


public class HbaseJsonEventSerializerTest extends AbstractSearchJUnit4SpringContextTests{

	private HbaseJsonEventSerializer hbaseJsonEventSerializer;
	@Autowired
	private GenerateSearchAnalyticsDataService generateSearchAnalyticsDataService;
	
	@Before
	public void setup()
	{
		hbaseJsonEventSerializer = new HbaseJsonEventSerializer();
	}
	
	@Test
	public void testInitialize() throws JsonProcessingException, UnknownHostException {
		Event event = generateSearchAnalyticsDataService.getSearchEvents(1).get(0);
		hbaseJsonEventSerializer.initialize(event, null);
		System.out.println(hbaseJsonEventSerializer.getSearchQueryInstruction());
		System.out.println(hbaseJsonEventSerializer.getHeaders());
	}

	@Test
	public void testGetActions() {
		Event event = generateSearchAnalyticsDataService.getSearchEvents(1).get(0);
		hbaseJsonEventSerializer.initialize(event, null);
		Put action = (Put) hbaseJsonEventSerializer.getActions().get(0);
		byte[] row = action.getRow();
		System.out.println(new String(row));
		System.out.println(new String(row.clone()));
	}

}
