package org.jai.oozie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClientException;
import org.jai.hive.HiveSearchClicksService;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class OozieJobsServiceImplTest extends
		AbstractSearchJUnit4SpringContextTests {

	private static final String TBNAME_SEARCH_CLICKS = "search_clicks";
	private static final String DBNAME_SEARCH = "search";
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
			generateSearchAnalyticsDataService.generateAndPushSearchEvents(200);
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void testAddHiveActionWorkflowJob() throws OozieClientException,
			InterruptedException, IllegalArgumentException, IOException {
		prepareHiveData();
		oozieJobsService.runHiveAddPartitionWorkflowJob();
		System.out.println("Workflow job completed ...");
		int totalRowCount = hiveSearchClicksService.getTotalRowCount(
				DBNAME_SEARCH, TBNAME_SEARCH_CLICKS);
		System.out.println("totalRowCount : " + totalRowCount);
		assertTrue(totalRowCount > 0);
	}

	@Test
	public void startHiveAddPartitionCoordJob() throws OozieClientException,
			InterruptedException, IllegalArgumentException, IOException {
		prepareHiveData();
		int itemsCount = printAndCountHdfsFileDirData(
				hadoopClusterService.getHDFSUri() + "/searchevents",
				"searchevents", false, true);
		assertEquals(200, itemsCount);
		hiveSearchClicksService.printHivePartitions(DBNAME_SEARCH,
				TBNAME_SEARCH_CLICKS);
		oozieJobsService.startHiveAddPartitionCoordJob();
		System.out.println("Coord job completed ...");
		hiveSearchClicksService.printHivePartitions(DBNAME_SEARCH,
				TBNAME_SEARCH_CLICKS);
		int totalRowCount = hiveSearchClicksService.getTotalRowCount(
				DBNAME_SEARCH, TBNAME_SEARCH_CLICKS);
		System.out.println("totalRowCount : " + totalRowCount);
		assertTrue(totalRowCount > 0);
	}

	@Test
	@Ignore
	public void startIndexTopCustomerQueryBundleCoordJob()
			throws OozieClientException, InterruptedException,
			IllegalArgumentException, IOException {
		//Make sure search_clicks data exists.
		prepareHiveData();
		int itemsCount = printAndCountHdfsFileDirData(
				hadoopClusterService.getHDFSUri() + "/searchevents",
				"searchevents", false, true);
		assertEquals(200, itemsCount);
		hiveSearchClicksService.printHivePartitions(DBNAME_SEARCH,
				TBNAME_SEARCH_CLICKS);
		oozieJobsService.startHiveAddPartitionCoordJob();
		System.out.println("Coord job completed ...");
		hiveSearchClicksService.printHivePartitions(DBNAME_SEARCH,
				TBNAME_SEARCH_CLICKS);
		int totalRowCount = hiveSearchClicksService.getTotalRowCount(
				DBNAME_SEARCH, TBNAME_SEARCH_CLICKS);
		System.out.println("totalRowCount : " + totalRowCount);
		assertTrue(totalRowCount > 0);
		
		oozieJobsService.startIndexTopCustomerQueryBundleCoordJob();
		System.out.println("Bundle job completed ...");
		int totalQueryRowCount = hiveSearchClicksService.getTotalRowCount(
				DBNAME_SEARCH, "search_customerquery");
		System.out.println("totalRowCount : " + totalRowCount);
		assertTrue(totalQueryRowCount > 0);
	}
}
