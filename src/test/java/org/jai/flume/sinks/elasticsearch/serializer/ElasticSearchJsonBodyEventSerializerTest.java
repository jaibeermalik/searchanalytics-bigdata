package org.jai.flume.sinks.elasticsearch.serializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.flume.Event;
import org.apache.flume.event.JSONEvent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Test;

public class ElasticSearchJsonBodyEventSerializerTest {
	ElasticSearchJsonBodyEventSerializer esSerializer = new ElasticSearchJsonBodyEventSerializer();

	@Test
	public void testESJsonEventSerializer() throws IOException {
		final Event event = new JSONEvent();
		final String writeValueAsString = "{\"hostedmachinename\":\"172.16.9.582\",\"pageurl\":\"http://blahblah:/1881\",\"customerid\":376,\"sessionid\":\"1eaa6cd1-0a71-4d03-aea4-d038921f5c6a\",\"querystring\":null,\"sortorder\":\"asc\",\"pagenumber\":0,\"totalhits\":39,\"hitsshown\":11,\"timestamp\":1397220014988,\"clickeddocid\":null,\"filters\":[{\"code\":\"specification_resolution\",\"value\":\"1024 x 600\"},{\"code\":\"searchfacettype_product_type_level_2\",\"value\":\"Laptops\"}]}";
		event.setBody(writeValueAsString.getBytes());
		final Map<String, String> headers = new HashMap<String, String>();
		headers.put("eventId", UUID.randomUUID().toString());
		event.setHeaders(headers);
		((XContentBuilder) esSerializer.getContentBuilder(event)).string();
	}
}
