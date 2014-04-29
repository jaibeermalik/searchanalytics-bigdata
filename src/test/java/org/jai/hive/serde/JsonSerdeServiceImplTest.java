package org.jai.hive.serde;

import static org.junit.Assert.*;

import org.junit.Test;

public class JsonSerdeServiceImplTest {

	private JsonSerdeServiceImpl jsonSerdeServiceImpl = new JsonSerdeServiceImpl();
	@Test
	public void testGetJsonJarPath() {
		System.out.println(jsonSerdeServiceImpl.getJsonJarPath());
	}

}
