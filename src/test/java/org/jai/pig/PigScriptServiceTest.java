package org.jai.pig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.joda.time.DateTime;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class PigScriptServiceTest extends
		AbstractSearchJUnit4SpringContextTests {

	private int searchEventsCount = 10;
	@Autowired
	private PigScriptService pigScriptService;

	private void prepareSearchEventsData() {
		try {
			boolean delete = hadoopClusterService.getFileSystem().delete(
					new Path("/searchevents"), true);
			if (!delete)
			{
				System.out.println("Error cleaning up hadoop dir!");
				fail();
			}
			generateSearchAnalyticsDataService
					.generateAndPushSearchEvents(searchEventsCount);
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void countTotalEvents() {
		prepareSearchEventsData();
		long countTotalEvents = pigScriptService.countTotalEvents();
		assertEquals(Long.valueOf(searchEventsCount).longValue(),
				countTotalEvents);
	}

	@Test
	public void getAllCustomerIds() throws IllegalArgumentException,
			IOException {
		prepareSearchEventsData();

		DateTime now = new DateTime();
		int monthOfYear = now.getMonthOfYear();
		int dayOfMonth = now.getDayOfMonth();
		int hourOfDay = now.getHourOfDay();
		String year = String.valueOf(now.getYear());
		String month = monthOfYear < 10 ? "0" + String.valueOf(monthOfYear)
				: String.valueOf(monthOfYear);
		String day = dayOfMonth < 10 ? "0" + String.valueOf(dayOfMonth)
				: String.valueOf(dayOfMonth);
		String hour = hourOfDay < 10 ? "0" + String.valueOf(hourOfDay) : String
				.valueOf(hourOfDay);
		pigScriptService.getAllCustomerIds(year, month, day, hour);

		String pigJobOutPath = "/tmp/PIGSEARCHEVENTS_data_" + year + "_"
				+ month + "_" + day + "_" + hour;
		assertEquals(searchEventsCount,
				countHdfsFileDataRecords(pigJobOutPath, "part"));
	}

}
