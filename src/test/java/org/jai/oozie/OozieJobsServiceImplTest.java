package org.jai.oozie;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClientException;
import org.jai.hadoop.HadoopClusterService;
import org.jai.hive.HiveSearchClicksService;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class OozieJobsServiceImplTest extends
		AbstractSearchJUnit4SpringContextTests {

	@Autowired
	private OozieJobsService oozieJobsService;
	@Autowired
	private HiveSearchClicksService hiveSearchClicksService;
	
	@Before
	public void prepareHive() {
		hiveSearchClicksService.setup();
	}

	private void prepareHiveData() {
		try {
			hadoopClusterService.getFileSystem().delete(
					new Path("/searchevents"), true);
			generateSearchAnalyticsDataService
					.generateAndPushSearchEvents(10);
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void testAddHiveActionWorkflowJob() throws OozieClientException, InterruptedException,
			IllegalArgumentException, IOException {
		prepareHiveData();
		 oozieJobsService.runHiveAddPartitionWorkflowJob();
		System.out.println("Workflow job completed ...");
		int totalRowCount = hiveSearchClicksService.getTotalRowCount("search", "search_clicks");
		System.out.println("totalRowCount : " + totalRowCount);
		assertTrue(totalRowCount > 0);
	}
	
	@Test
	@Ignore
	public void startHiveAddPartitionCoordJob() throws OozieClientException, InterruptedException,
			IllegalArgumentException, IOException {
		prepareHiveData();
		 oozieJobsService.startHiveAddPartitionCoordJob();
		System.out.println("Coord job completed ...");
		int totalRowCount = hiveSearchClicksService.getTotalRowCount("search", "search_clicks");
		System.out.println("totalRowCount : " + totalRowCount);
		assertTrue(totalRowCount > 0);
	}
	
	@Test
	@Ignore
	public void startIndexTopCustomerQueryBundleCoordJob() throws OozieClientException, InterruptedException,
			IllegalArgumentException, IOException {
		prepareHiveData();
		 oozieJobsService.startIndexTopCustomerQueryBundleCoordJob();
		System.out.println("Bundle job completed ...");
		int totalRowCount = hiveSearchClicksService.getTotalRowCount("search", "search_customerquery");
		System.out.println("totalRowCount : " + totalRowCount);
		assertTrue(totalRowCount > 0);
	}
}
