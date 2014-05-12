package org.jai.search.test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.jai.hadoop.HadoopClusterService;
import org.jai.search.actors.BootStrapIndexService;
import org.jai.search.analytics.GenerateSearchAnalyticsDataService;
import org.jai.search.client.SearchClientService;
import org.jai.search.config.ElasticSearchIndexConfig;
import org.jai.search.data.SampleDataGeneratorService;
import org.jai.search.index.IndexProductDataService;
import org.jai.search.query.ProductQueryService;
import org.jai.search.setup.SetupIndexService;
import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

@ContextConfiguration(locations = { "classpath:applicationContext-elasticsearch.xml" })
public abstract class AbstractSearchJUnit4SpringContextTests extends
		AbstractJUnit4SpringContextTests {
	@Autowired
	@Qualifier("searchClientService")
	protected SearchClientService searchClientService;
	@Autowired
	protected SetupIndexService setupIndexService;
	@Autowired
	protected SampleDataGeneratorService sampleDataGeneratorService;
	@Autowired
	protected ProductQueryService productQueryService;
	@Autowired
	protected IndexProductDataService indexProductData;
	@Autowired
	protected BootStrapIndexService bootStrapIndexService;
	@Autowired
	protected GenerateSearchAnalyticsDataService generateSearchAnalyticsDataService;
	@Autowired
	protected HadoopClusterService hadoopClusterService;

	protected Client getClient() {
		return searchClientService.getClient();
	}

	@Before
	public void prepare() {
		try {
			bootStrapIndexService.preparingIndexes();
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}

		searchClientService.getClient().admin().indices()
				.refresh(Requests.refreshRequest()).actionGet();
		System.out.println("yes, test setup indexing preparation done!");
	}

	protected void refreshSearchServer() {
		searchClientService.getClient().admin().indices()
				.refresh(Requests.refreshRequest()).actionGet();
	}

	protected void checkIndexHealthStatus(String indexName) {
		ClusterHealthRequest request = new ClusterHealthRequest(indexName);
		ClusterHealthStatus clusterHealthStatus = searchClientService
				.getClient().admin().cluster().health(request).actionGet()
				.getStatus();

		assertTrue(clusterHealthStatus.equals(ClusterHealthStatus.GREEN));
	}

	protected long getIndexTotalDocumentCount(
			ElasticSearchIndexConfig elasticSearchIndexConfig) {
		long count = searchClientService.getClient()
				.prepareCount(elasticSearchIndexConfig.getIndexAliasName())
				.setTypes(elasticSearchIndexConfig.getDocumentType()).execute()
				.actionGet().getCount();

		return count;
	}

	protected void printHdfsData() throws IOException {
		printHdfsFileDirData(hadoopClusterService.getHDFSUri()
				+ "/searchevents", "searchevents");
	}
	
	protected void printHdfsFileDirData(String path, String filePrefix) throws IOException {
		printAndCountHdfsFileDirData(path, filePrefix, true, false);
	}
	
	protected int printAndCountHdfsFileDirData(String path, String filePrefix, boolean print, boolean count) throws IOException {
		int recordsCount =0;
		DistributedFileSystem fs = hadoopClusterService.getFileSystem();
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(path), true);
		while (files.hasNext()) {
			LocatedFileStatus locatedFileStatus = files.next();
			System.out.println("Check:" + locatedFileStatus.getPath());
			if (locatedFileStatus.isFile()) {
				Path filePath = locatedFileStatus.getPath();
				if (filePath.getName().startsWith(filePrefix)) {
					FSDataInputStream input = fs.open(filePath);
					BufferedReader reader = new BufferedReader(
							new InputStreamReader(input));
					String body = null;
					while ((body = reader.readLine()) != null) {
						if(print)
						{
							System.out.println("body is:" + body);
						}
						if(count)
						{
							recordsCount++;
						}
					}
					reader.close();
					input.close();
				}
			}
		}
		return recordsCount;
	}
	
	protected int countHdfsFileDataRecords(String path, String filePrefix) throws IOException {
		return printAndCountHdfsFileDirData(path, filePrefix, false, true);
	}
}
