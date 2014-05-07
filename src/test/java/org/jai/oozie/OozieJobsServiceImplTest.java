package org.jai.oozie;

import java.io.IOException;

import org.apache.oozie.client.OozieClientException;
import org.jai.hadoop.HadoopClusterService;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class OozieJobsServiceImplTest extends
		AbstractSearchJUnit4SpringContextTests {

	@Autowired
	private OozieJobsService oozieJobsService;
	@Autowired
	private HadoopClusterService hadoopClusterService;

	@Test
	public void test() throws OozieClientException, InterruptedException,
			IllegalArgumentException, IOException {
		// oozieJobsService.setup();
		// oozieJobsService.startHiveAddPartitionCoordJob();
		// oozieJobsService.startHiveAddPartitionCoordJob();
		// oozieJobsService.setup();
		// print the final status o the workflow job
		System.out.println("Workflow job completed ...");
		// oozieJobsService.shutdown();
	}
}
