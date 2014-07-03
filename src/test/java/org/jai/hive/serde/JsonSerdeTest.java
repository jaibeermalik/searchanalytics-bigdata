package org.jai.hive.serde;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaIntObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;

public class JsonSerdeTest {
	JSONSerDe jsonSerde = new JSONSerDe();

	@Before
	public void initialize() throws Exception {
		final Configuration conf = null;
		final Properties tbl = new Properties();
		tbl.setProperty(serdeConstants.LIST_COLUMNS,
				"hostedmachinename,pageurl,customerid,timestamp,filters");
		tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES,
				"string,string,int,bigint,array<struct<code:string,value:string>>");
		jsonSerde.initialize(conf, tbl);
	}

	@Test
	public void testDeserialize() throws SerDeException {
		final String writeValueAsString = "{\"hostedmachinename\":\"172.16.9.582\",\"pageurl\":\"http://blahblah:/1881\",\"customerid\":376,\"sessionid\":\"1eaa6cd1-0a71-4d03-aea4-d038921f5c6a\",\"querystring\":null,\"sortorder\":\"asc\",\"pagenumber\":0,\"totalhits\":39,\"hitsshown\":11,\"timestamp\":1397220014988,\"clickeddocid\":null,\"filters\":[{\"code\":\"specification_resolution\",\"value\":\"1024 x 600\"},{\"code\":\"searchfacettype_product_type_level_2\",\"value\":\"Laptops\"}]}";
		final Writable text = new Text(writeValueAsString);
		final Object result = jsonSerde.deserialize(text);
		final StructObjectInspector soi = (StructObjectInspector) jsonSerde
				.getObjectInspector();
		assertEquals(
				"172.16.9.582",
				soi.getStructFieldData(result,
						soi.getStructFieldRef("hostedmachinename")));
		final JavaIntObjectInspector jIntOI = (JavaIntObjectInspector) soi
				.getStructFieldRef("customerid").getFieldObjectInspector();
		assertTrue(376 == jIntOI.get(soi.getStructFieldData(result,
				soi.getStructFieldRef("customerid"))));
		final Object ar = soi.getStructFieldData(result,
				soi.getStructFieldRef("filters"));
		assertNotNull(ar);
		soi.getStructFieldRef("filters").getFieldObjectInspector();
		// System.out.println(soi2.getListLength(soi2));
		System.out.println(result);
		// assertTrue(result instanceof org.json.JSONArray);
	}
}
