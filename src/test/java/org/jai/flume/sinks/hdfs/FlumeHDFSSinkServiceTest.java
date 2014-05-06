package org.jai.flume.sinks.hdfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.flume.Event;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.jai.hadoop.HadoopClusterService;
import org.jai.search.analytics.GenerateSearchAnalyticsDataService;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class FlumeHDFSSinkServiceTest extends AbstractSearchJUnit4SpringContextTests{

	@Autowired
	private GenerateSearchAnalyticsDataService generateSearchAnalyticsDataService;
	@Autowired
	private FlumeHDFSSinkService flumeHDFSSinkService;
	@Autowired
	private HadoopClusterService hadoopClusterService;
	
	@Test
	public void testProcessEvents() throws FileNotFoundException, IOException {
		int searchEventsCount = 101;
		List<Event> searchEvents = generateSearchAnalyticsDataService
				.getSearchEvents(searchEventsCount);
		
		flumeHDFSSinkService.processEvents(searchEvents);

		// list all files and check data.
		Path dirPath = new Path(hadoopClusterService.getHDFSUri()
				+ "/searchevents");
		// FileStatus[] dirStat = fs.listStatus(dirPath);
		// Path fList[] = FileUtil.stat2Paths(dirStat);

		DistributedFileSystem fs = hadoopClusterService.getFileSystem();
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(dirPath, true);
		while (files.hasNext()) {
			LocatedFileStatus locatedFileStatus = files.next();
			System.out.println("Check:" + locatedFileStatus.getPath());
			if (locatedFileStatus.isFile()) {
				Path path = locatedFileStatus.getPath();
				if (path.getName().startsWith("searchevents")) {
					FSDataInputStream input = fs.open(path);
					BufferedReader reader = new BufferedReader(
							new InputStreamReader(input));
					String body = null;
					while ((body = reader.readLine()) != null) {
						System.out.println("body is:" + body);
					}
					reader.close();
					input.close();
				}
			}
		}
	}

}
