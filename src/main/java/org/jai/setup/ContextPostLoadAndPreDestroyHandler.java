package org.jai.setup;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.jai.flume.agent.FlumeAgentService;
import org.jai.flume.sinks.elasticsearch.FlumeESSinkService;
import org.jai.flume.sinks.hdfs.FlumeHDFSSinkService;
import org.jai.hadoop.HadoopClusterService;
import org.jai.hive.HiveSearchClicksService;
import org.jai.hive.serde.JsonSerdeService;
import org.jai.oozie.OozieJobsService;
import org.jai.search.client.SearchClientService;
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
	
	@PostConstruct
	public void start()
	{
		searchClientService.setup();
		//NOT required, may be for dependency jar later.
//		jsonSerdeService.build();
		hadoopClusterService.start();
		flumeESSinkService.start();
		flumeHDFSSinkService.start();
		flumeAgentService.setup();
//		hiveSearchClicksService.setup();
		oozieJobsService.setup();
	}

	@PreDestroy
	public void shutdown()
	{
		oozieJobsService.shutdown();
		flumeESSinkService.shutdown();
		flumeHDFSSinkService.shutdown();
		flumeAgentService.shutdown();

		//wait 5 sec for others to get closed.
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
//			throw RuntimeException()
		}
		hadoopClusterService.shutdown();
		actorSystem.shutdown();
		searchClientService.shutdown();
	}

}
