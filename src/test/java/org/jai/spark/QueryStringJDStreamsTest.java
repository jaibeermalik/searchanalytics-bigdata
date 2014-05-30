package org.jai.spark;

import static org.junit.Assert.*;

import org.junit.Test;

public class QueryStringJDStreamsTest {

	private QueryStringJDStreams queryStringJDStreams;

	@Test
	public void testGetQueryString() {
		queryStringJDStreams = new QueryStringJDStreams();
		String eventString = "{\"eventid\":\"55055db4-b88f-4e7f-a0a8-3d038e6ce539-1401443638367-449\",\"hostedmachinename\":\"192.168.182.1330\",\"pageurl\":\"http://blahblah:/0\",\"customerid\":449,\"sessionid\":\"110786f8-f26c-4cc5-b4b4-f8262532c1ac\",\"querystring\":\"querystring123\",\"sortorder\":\"desc\",\"pagenumber\":3,\"totalhits\":15,\"hitsshown\":35,\"createdtimestampinmillis\":1401443638367,\"clickeddocid\":\"22\",\"favourite\":true,\"eventidsuffix\":\"55055db4-b88f-4e7f-a0a8-3d038e6ce539\",\"filters\":[{\"code\":\"searchfacettype_age_level_2\",\"value\":\"12-18 years\"},{\"code\":\"searchfacettype_color_level_2\",\"value\":\"Green\"}]}";
		String queryString = queryStringJDStreams.getQueryString(eventString);
		assertEquals("querystring123", queryString);
		
		String eventStringNull = "{\"eventid\":\"55055db4-b88f-4e7f-a0a8-3d038e6ce539-1401443638367-449\",\"hostedmachinename\":\"192.168.182.1330\",\"pageurl\":\"http://blahblah:/0\",\"customerid\":449,\"sessionid\":\"110786f8-f26c-4cc5-b4b4-f8262532c1ac\",\"querystring\":null,\"sortorder\":\"desc\",\"pagenumber\":3,\"totalhits\":15,\"hitsshown\":35,\"createdtimestampinmillis\":1401443638367,\"clickeddocid\":\"22\",\"favourite\":true,\"eventidsuffix\":\"55055db4-b88f-4e7f-a0a8-3d038e6ce539\",\"filters\":[{\"code\":\"searchfacettype_age_level_2\",\"value\":\"12-18 years\"},{\"code\":\"searchfacettype_color_level_2\",\"value\":\"Green\"}]}";
		String queryStringNull = queryStringJDStreams.getQueryString(eventStringNull);
		assertNull(queryStringNull);
	}
	
	@Test
	public void testProductIdString() {
		queryStringJDStreams = new QueryStringJDStreams();
		String eventString = "{\"eventid\":\"55055db4-b88f-4e7f-a0a8-3d038e6ce539-1401443638367-449\",\"hostedmachinename\":\"192.168.182.1330\",\"pageurl\":\"http://blahblah:/0\",\"customerid\":449,\"sessionid\":\"110786f8-f26c-4cc5-b4b4-f8262532c1ac\",\"querystring\":\"querystring123\",\"sortorder\":\"desc\",\"pagenumber\":3,\"totalhits\":15,\"hitsshown\":35,\"createdtimestampinmillis\":1401443638367,\"clickeddocid\":\"22\",\"favourite\":true,\"eventidsuffix\":\"55055db4-b88f-4e7f-a0a8-3d038e6ce539\",\"filters\":[{\"code\":\"searchfacettype_age_level_2\",\"value\":\"12-18 years\"},{\"code\":\"searchfacettype_color_level_2\",\"value\":\"Green\"}]}";
		String productIdString = queryStringJDStreams.getProductIdString(eventString);
		assertEquals("22", productIdString);
		
		String eventStringNull = "{\"eventid\":\"55055db4-b88f-4e7f-a0a8-3d038e6ce539-1401443638367-449\",\"hostedmachinename\":\"192.168.182.1330\",\"pageurl\":\"http://blahblah:/0\",\"customerid\":449,\"sessionid\":\"110786f8-f26c-4cc5-b4b4-f8262532c1ac\",\"querystring\":null,\"sortorder\":\"desc\",\"pagenumber\":3,\"totalhits\":15,\"hitsshown\":35,\"createdtimestampinmillis\":1401443638367,\"clickeddocid\":null,\"favourite\":true,\"eventidsuffix\":\"55055db4-b88f-4e7f-a0a8-3d038e6ce539\",\"filters\":[{\"code\":\"searchfacettype_age_level_2\",\"value\":\"12-18 years\"},{\"code\":\"searchfacettype_color_level_2\",\"value\":\"Green\"}]}";
		String productIdStringNull = queryStringJDStreams.getProductIdString(eventStringNull);
		assertNull(productIdStringNull);
	}
}
