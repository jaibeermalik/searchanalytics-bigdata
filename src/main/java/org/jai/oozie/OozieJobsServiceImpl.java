package org.jai.oozie;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.jai.hadoop.HadoopClusterService;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

@Service
public class OozieJobsServiceImpl implements OozieJobsService {

	private static final Logger LOG = LoggerFactory
			.getLogger(OozieJobsServiceImpl.class);

	@Autowired
	private HadoopClusterService hadoopClusterService;

	@Override
	public void startHiveAddPartitionCoordJob() {
		try {
			String workFlowRoot = hadoopClusterService.getHDFSUri()
					+ "/usr/tom/oozie/wf-hive-add-partition";

			// put oozie app in hadoop
			DistributedFileSystem fs = hadoopClusterService.getFileSystem();
			fs.delete(new Path(workFlowRoot), true);

			File wfDir = new ClassPathResource("oozie/wf-hive-add-partition")
					.getFile();
			LOG.debug("wfdir: {}", wfDir.getAbsolutePath());
			FileUtil.copy(wfDir, fs, new Path(workFlowRoot), false,
					new Configuration());

			// submitCoordJob(workFlowRoot);
			submitWorkflowJob(workFlowRoot);

		} catch (OozieClientException | InterruptedException
				| IllegalArgumentException | IOException e) {
			String errMsg = "Error occured while starting hive add partition coord job!";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
	}

	private void submitCoordJob(String workFlowRoot)
			throws OozieClientException, InterruptedException {
		OozieClient client = LocalOozie.getCoordClient();
		Properties conf = client.createConfiguration();
		conf.setProperty(OozieClient.COORDINATOR_APP_PATH, workFlowRoot
				+ "/coord-app-hive-add-partition.xml");
		conf.setProperty("nameNode", hadoopClusterService.getHDFSUri());
		conf.setProperty("jobTracker", hadoopClusterService.getJobTRackerUri());
		conf.setProperty("workflowRoot", workFlowRoot);
		Date now = new Date();
		conf.setProperty("jobStart", DateUtils.formatDateOozieTZ(now));
		conf.setProperty("jobEnd", DateUtils.formatDateOozieTZ(new DateTime()
				.plusHours(2).toDate()));
		conf.setProperty("initialDataset", DateUtils.formatDateOozieTZ(now));
		conf.setProperty("tzOffset", "1");

		// submit and start the workflow job
		String jobId = client.run(conf);// submit(conf);

		LOG.debug("Workflow job submitted");
		// wait until the workflow job finishes printing the status every 10
		// seconds
		CoordinatorJob coordJobInfo = client.getCoordJobInfo(jobId);
		while (coordJobInfo.getStatus() == Job.Status.RUNNING) {
			LOG.debug("Workflow job running ...");
			LOG.debug("coordJobInfo StartTime: {}", coordJobInfo.getStartTime());
			LOG.debug("coordJobInfo NextMaterizedTime: {}",
					coordJobInfo.getNextMaterializedTime());
			LOG.debug("coordJobInfo EndTime: {}", coordJobInfo.getEndTime());
			LOG.debug("coordJobInfo Frequency: {}", coordJobInfo.getFrequency());
			LOG.debug("coordJobInfo ConsoleURL: {}",
					coordJobInfo.getConsoleUrl());
			LOG.debug("coordJobInfo Status: {}", coordJobInfo.getStatus());
			LOG.debug("coordJobInfo ActionConsoleURL: {}", coordJobInfo
					.getActions().get(0).getConsoleUrl());
			LOG.debug("coordJobInfo ActionErrorMessage: {}", coordJobInfo
					.getActions().get(0).getErrorMessage());
			Thread.sleep(10 * 1000);
		}
	}

	private void submitWorkflowJob(String workFlowRoot)
			throws OozieClientException, InterruptedException {
		OozieClient client = LocalOozie.getClient();
		Properties conf = client.createConfiguration();
		conf.setProperty(OozieClient.APP_PATH, workFlowRoot
				+ "/hive-action-add-partition.xml");
		conf.setProperty("nameNode", hadoopClusterService.getHDFSUri());
		conf.setProperty("jobTracker", hadoopClusterService.getJobTRackerUri());
		conf.setProperty("workflowRoot", workFlowRoot);
		Date now = new Date();
		conf.setProperty("jobStart", DateUtils.formatDateOozieTZ(now));
		conf.setProperty("jobEnd", DateUtils.formatDateOozieTZ(new DateTime()
				.plusHours(2).toDate()));
		conf.setProperty("initialDataset", DateUtils.formatDateOozieTZ(now));
		conf.setProperty("tzOffset", "1");

		// submit and start the workflow job
		String jobId = client.run(conf);// submit(conf);

		LOG.debug("Workflow job submitted");
		// wait until the workflow job finishes printing the status every 10
		// seconds
		WorkflowJob jobInfo = client.getJobInfo(jobId);
		int i = 1;
		statuscheck: while (jobInfo.getStatus() != WorkflowJob.Status.SUCCEEDED) {
			LOG.debug("Workflow job running ...");
			LOG.debug("coordJobInfo StartTime: {}", jobInfo.getStartTime());
			LOG.debug("coordJobInfo EndTime: {}", jobInfo.getEndTime());
			LOG.debug("coordJobInfo ConsoleURL: {}", jobInfo.getConsoleUrl());
			LOG.debug("coordJobInfo Status: {}", jobInfo.getStatus());
			WorkflowAction workflowAction = jobInfo.getActions().get(0);
			LOG.debug("coordJobInfo Action consoleURL: {}",
					workflowAction.getConsoleUrl());
			LOG.debug("coordJobInfo Action Name: {}", workflowAction.getName());
			LOG.debug("coordJobInfo Action error message: {}",
					workflowAction.getErrorMessage());
			LOG.debug("coordJobInfo Action Status: {}",
					workflowAction.getStats());
			LOG.debug("coordJobInfo Action data: {}", workflowAction.getData());
			LOG.debug("coordJobInfo Action conf: {}", workflowAction.getConf());
			LOG.debug("coordJobInfo Action retries: {}",
					workflowAction.getRetries());
			LOG.debug("coordJobInfo Action id: {}", workflowAction.getId());
			LOG.debug("coordJobInfo Action start time: {}",
					workflowAction.getStartTime());
			LOG.debug("coordJobInfo Action end time: {}",
					workflowAction.getEndTime());
			LOG.debug("coordJobInfo Oozie Url: {}", client.getOozieUrl());
			Thread.sleep(10 * 1000);
			i++;
			if (i == 2)
				break statuscheck;
		}
	}

	@Override
	public void setup() {

		// Clean up and set up oozie home stuff

		File oozieHome = new File("target/ooziehome");
		File oozieData = new File(oozieHome, "data");
		System.setProperty(Services.OOZIE_HOME_DIR, oozieHome.getAbsolutePath());
		System.setProperty("oozie.data.dir", oozieData.getAbsolutePath());

		try {
			oozieHome.delete();
			oozieHome.mkdir();
			File oozieConf = new File(oozieHome, "conf");
			oozieConf.mkdir();
			FileUtils.copyFileToDirectory(new ClassPathResource(
					"conf/oozie-site.xml").getFile(), oozieConf);
			File oozieHadoopConf = new File(oozieConf, "hadoop-conf");
			oozieHadoopConf.mkdir();
			FileUtils.copyFileToDirectory(new ClassPathResource(
					"conf/core-site.xml").getFile(), oozieConf);
			File oozieActionConf = new File(oozieConf, "action-conf");
			oozieActionConf.mkdir();
			FileUtils.copyFileToDirectory(new ClassPathResource(
					"conf/hive-site.xml").getFile(), oozieConf);
			oozieData.mkdir();
		} catch (IOException e) {
			String errMsg = "Error setting up oozie home!";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}

		// System.setProperty("oozie.service.JPAService.jdbc.driver",
		// "org.apache.derby.jdbc.EmbeddedDriver");
		// System.setProperty("oozie.service.JPAService.jdbc.url",
		// "jdbc:derby:${oozie.data.dir}/oozie-db;create=true");
		// System.setProperty("oozie.service.JPAService.jdbc.username", "sa");
		// System.setProperty("oozie.service.JPAService.jdbc.password", "");
		// System.setProperty("oozie.service.JPAService.create.db.schema",
		// "true");
		// System.setProperty("oozie.service.ActionService.executor.ext.classes",
		// "org.apache.oozie.action.hadoop.HiveActionExecutor");
		// System.setProperty("oozie.service.SchemaService.wf.ext.schemas",
		// "hive-action-0.3.xsd,hive-action-0.4.xsd,hive-action-0.5.xsd,"
		// + "oozie-sla-0.1.xsd,oozie-sla-0.2.xsd");
		// System.setProperty("oozie.base.url", "http://localhost:54210");
		System.setProperty("OOZIE_HTTP_HOSTNAME", "localhost");
		System.setProperty("OOZIE_HTTP_PORT", "54210");
		System.setProperty("oozie.data.dir", oozieData.getAbsolutePath());
		System.setProperty("oozielocal.log", new File(
				"target/logs/oozielocal.log").getAbsolutePath());

		// Start local oozie.
		try {
			LocalOozie.start();
		} catch (Exception e) {
			e.printStackTrace();
			String errMsg = "Error occured while starting local oozie!";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
	}

	@Override
	public void shutdown() {
		LocalOozie.stop();
	}

}
