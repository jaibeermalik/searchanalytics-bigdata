package org.jai.hadoop.example;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.apache.flume.sink.elasticsearch.ElasticSearchSink;
import org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants;
import org.apache.flume.sink.hdfs.HDFSEventSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.jai.search.analytics.GenerateSearchAnalyticsDataService;
import org.jai.search.client.SearchClientService;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class WriteDataToFileTest extends AbstractSearchJUnit4SpringContextTests {

	@Autowired
	private GenerateSearchAnalyticsDataService generateSearchAnalyticsDataService;
	@Autowired
	private SearchClientService searchClientService;

	@Test
	public void test() {
		try {

			// Start cluster
			MiniDFSCluster hdfsCluster = startAndGEtHdfsCluster();
			String hdfsURI = "hdfs://localhost.localdomain:"
					+ hdfsCluster.getNameNodePort();

			System.out.println("HDFS url is " + hdfsURI);
			DistributedFileSystem fs = hdfsCluster.getFileSystem();

			// Generate Events
			List<Event> searchEvents = generateSearchAnalyticsDataService
					.getSearchEvents(11);

			// Write events to file and read.
//			hdfsFileLoggerSinkAndTest(fs, searchEvents);

			// Write events to hdfs sink and test data.
//			FlumehdfsSinkAndTestData(hdfsURI, fs, searchEvents);
			
			FlumeESSinkAndTestData(searchEvents);

		} catch (EventDeliveryException | InterruptedException | IOException e) {
			e.printStackTrace();
			fail();
		}
	}
	
	private void FlumeESSinkAndTestData(List<Event> searchEvents)
			throws EventDeliveryException, IOException, FileNotFoundException {

		ElasticSearchSink sink = new ElasticSearchSink();

		Map<String, String> paramters = new HashMap<>();
		paramters.put(ElasticSearchSinkConstants.HOSTNAMES, "127.0.0.1:9310");
		String indexNamePrefix = "recentlyviewed";
		paramters.put(ElasticSearchSinkConstants.INDEX_NAME, indexNamePrefix);
		paramters.put(ElasticSearchSinkConstants.INDEX_TYPE, "clickevent");
		paramters.put(ElasticSearchSinkConstants.CLUSTER_NAME, "jai-testclusterName");
		paramters.put(ElasticSearchSinkConstants.BATCH_SIZE, "10");
		paramters.put(ElasticSearchSinkConstants.SERIALIZER, "org.jai.flume.sinks.elasticsearch.serializer.ElasticSearchJsonBodyEventSerializer");

		Context sinkContext = new Context(paramters);
		sink.configure(sinkContext);
		Channel channel = new MemoryChannel();
		Configurables.configure(channel, sinkContext);
		sink.setChannel(channel);
		sink.start();
		channel.start();

		// TODO: do this in batches.
		Transaction txn = channel.getTransaction();
		txn.begin();
		for (Event event : searchEvents) {
			channel.put(event);
			System.out.println("Putting event to channel: " + event);
		}
		txn.commit();
		txn.close();
		
		sink.process();
		
		channel.stop();
		sink.stop();

		Client client = searchClientService.getClient();
		client.admin().indices().refresh(Requests.refreshRequest()).actionGet();

		String indexName = indexNamePrefix + '-' + ElasticSearchIndexRequestBuilderFactory.df.format(new Date());
		long totalCount = client.prepareCount(indexName).get().getCount();
		System.out.println("Search total count is: " + totalCount);
		
		SearchHits hits = client.prepareSearch(indexName).get().getHits();
		System.out.println("Total hits: " + hits.getTotalHits());
		for (SearchHit searchHit : hits) {
			System.out.println(searchHit.getSource());
		}
	}

	private void FlumehdfsSinkAndTestData(String hdfsURI,
			DistributedFileSystem fs, List<Event> searchEvents)
			throws EventDeliveryException, IOException, FileNotFoundException {

		HDFSEventSink sink = new HDFSEventSink();

		Map<String, String> paramters = new HashMap<>();
		paramters.put("hdfs.type", "hdfs");
		String hdfsBasePath = hdfsURI + "/searchevents";
		paramters.put("hdfs.path", hdfsBasePath + "/%Y/%m/%d/%H");
		paramters.put("hdfs.filePrefix", "searchevents");
		// paramters.put("hdfs.inUsePrefix", "eventstemp");
		// paramters.put("hdfs.inUseSuffix", ".tmp");
		paramters.put("hdfs.fileType", "DataStream");
		// paramters.put("hdfs.writeFormat", "Text");
		paramters.put("hdfs.rollInterval", "0");
		// paramters.put("hdfs.rollSize", "134217728");
		paramters.put("hdfs.rollSize", "0");
		paramters.put("hdfs.idleTimeout", "60");
		paramters.put("hdfs.rollCount", "0");
		paramters.put("hdfs.batchSize", "10");
		paramters.put("hdfs.useLocalTimeStamp", "true");

		Context sinkContext = new Context(paramters);
		// sink.setName("HDFSEventSink-" + UUID.randomUUID().toString());
		sink.configure(sinkContext);
		Channel channel = new MemoryChannel();
		Configurables.configure(channel, sinkContext);
		sink.setChannel(channel);
		sink.start();
		channel.start();

		// TODO: do this in batches.
		Transaction txn = channel.getTransaction();
		txn.begin();
		for (Event event : searchEvents) {
			// String eventString = new String(event.getBody(), "UTF-8");
			// System.out.println("Writing event string: "+ eventString);
			channel.put(event);
			System.out.println("Putting event to channel: " + event);
			// channel.
			// out.writeUTF(eventString + System.lineSeparator());
		}
		txn.commit();
		txn.close();
		sink.process();
		channel.stop();
		sink.stop();

		// list all files and check data.
		Path dirPath = new Path(hdfsBasePath);
		// FileStatus[] dirStat = fs.listStatus(dirPath);
		// Path fList[] = FileUtil.stat2Paths(dirStat);

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
				}
			}
		}
	}

	private MiniDFSCluster startAndGEtHdfsCluster() throws IOException {
		String testName = "trial";
		File baseDir = new File("./target/hdfs/" + testName).getAbsoluteFile();
		FileUtil.fullyDelete(baseDir);
		Configuration conf = new Configuration();
		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		return builder.build();
	}

	private void hdfsFileLoggerSinkAndTest(DistributedFileSystem fs,
			List<Event> searchEvents) throws FileNotFoundException, IOException {

		// /Write to file
		Path outFile = new Path("/searchevents/event" + UUID.randomUUID());
		FSDataOutputStream out = fs.create(outFile, false);
		for (Event event : searchEvents) {
			String eventString = new String(event.getBody(), "UTF-8");
			System.out.println("Writing event string: " + eventString);
			out.writeUTF(eventString + System.lineSeparator());
		}
		out.flush();
		out.close();

		// check the data is there...with standard file
		FSDataInputStream input = fs.open(outFile);
		try (BufferedReader br = new BufferedReader(new InputStreamReader(
				input, "UTF-8"))) {
			String line = null;
			while ((line = br.readLine()) != null) {
				System.out.println("HDFS file line is:" + line);
			}
		}

		input.close();
		fs.delete(outFile, true);
	}

//	// not used
//	private List<String> getAllFiles(String input) {
//		// TODO: curretnly java local path Get hadoop path
//		System.out.println("Check for input:" + input);
//		List<String> output = Lists.newArrayList();
//		File dir = new File(input);
//		if (dir.isFile()) {
//			output.add(dir.getAbsolutePath());
//			System.out.println("Check for input is a file:"
//					+ dir.getAbsolutePath());
//		} else if (dir.isDirectory()) {
//			for (String file : dir.list()) {
//				File subDir = new File(dir, file);
//				System.out.println("Check for input is a sub dir:"
//						+ subDir.getAbsolutePath());
//				output.addAll(getAllFiles(subDir.getAbsolutePath()));
//			}
//		}
//		return output;
//	}
}
