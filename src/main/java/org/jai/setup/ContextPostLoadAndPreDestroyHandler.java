package org.jai.setup;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.jai.flume.agent.FlumeAgentService;
import org.jai.flume.sinks.elasticsearch.FlumeESSinkService;
import org.jai.flume.sinks.hbase.FlumeHbaseSinkService;
import org.jai.flume.sinks.hdfs.FlumeHDFSSinkService;
import org.jai.hadoop.HadoopClusterService;
import org.jai.hbase.HbaseService;
import org.jai.hive.HiveSearchClicksService;
import org.jai.hive.serde.JsonSerdeService;
import org.jai.oozie.OozieJobsService;
import org.jai.search.client.SearchClientService;
import org.jai.spark.SparkStreamService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import akka.actor.ActorSystem;

@Component
public class ContextPostLoadAndPreDestroyHandler {

	@Autowired
	private HadoopClusterService hadoopClusterService;
	@Autowired
	private FlumeAgentService flumeAgentService;
	@Autowired
	private FlumeESSinkService flumeESSinkService;
	@Autowired
	private FlumeHDFSSinkService flumeHDFSSinkService;
	@Autowired
	private JsonSerdeService jsonSerdeService;
	@Autowired
	private HiveSearchClicksService hiveSearchClicksService;
	@Autowired
	private OozieJobsService oozieJobsService;
	@Autowired
	private SearchClientService searchClientService;
	@Autowired
	private ActorSystem actorSystem;
	@Autowired
	private SparkStreamService sparkStreamService;
	@Autowired
	private HbaseService hbaseService;
	@Autowired
	private FlumeHbaseSinkService flumeHbaseSinkService;

	@PostConstruct
	public void start() {
		searchClientService.setup();
		// NOT required, may be for dependency jar later.
		// jsonSerdeService.build();
		hadoopClusterService.start();
		sparkStreamService.setup();
		hbaseService.setup();
		flumeESSinkService.start();
		flumeHDFSSinkService.start();
		flumeHbaseSinkService.start();
		flumeAgentService.setup();
		// hiveSearchClicksService.setup();
		oozieJobsService.setup();
	}

	@PreDestroy
	public void shutdown() {
		oozieJobsService.shutdown();
		sparkStreamService.shutdown();
		flumeESSinkService.shutdown();
		flumeHbaseSinkService.shutdown();
		flumeHDFSSinkService.shutdown();
		flumeAgentService.shutdown();

		// wait 5 sec for others to get closed.
//		try {
//			Thread.sleep(5000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//			// throw RuntimeException()
//		}
		hbaseService.shutdown();
		hadoopClusterService.shutdown();
		actorSystem.shutdown();
		searchClientService.shutdown();
	}

}
