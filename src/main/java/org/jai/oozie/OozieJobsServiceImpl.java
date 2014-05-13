package org.jai.oozie;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.WorkflowJob.Status;
import org.apache.oozie.service.CallbackService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;
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
			String workFlowRoot = setupHiveAddPartitionWorkflowApp();
			submitCoordJob(workFlowRoot);
		} catch (OozieClientException | InterruptedException
				| IllegalArgumentException | IOException e) {
			String errMsg = "Error occured while starting hive add partition coord job!";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
	}
	
	@Override
	public void runHiveAddPartitionWorkflowJob() {
		try {
			String workFlowRoot = setupHiveAddPartitionWorkflowApp();
			submitWorkflowJob(workFlowRoot);

		} catch (OozieClientException | InterruptedException
				| IllegalArgumentException | IOException e) {
			String errMsg = "Error occured while starting hive add partition Workflow job!";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
	}
	
	@Override
	public void startIndexTopCustomerQueryBundleCoordJob() {
		try {
			String workFlowRoot = setupTopCustomerQueryBundleJobApp();
			submitTopQueriesBundleCoordJob(workFlowRoot);
		} catch (OozieClientException | InterruptedException
				| IllegalArgumentException | IOException e) {
			String errMsg = "Error occured while starting bundle job!";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
	}
	
	private String setupTopCustomerQueryBundleJobApp() throws IOException {
		String userName = System.getProperty("user.name");
		String workFlowRoot = hadoopClusterService.getHDFSUri()
				+ "/usr/" + userName +"/oozie/bundle-hive-to-es-topqueries";

		// put oozie app in hadoop
		DistributedFileSystem fs = hadoopClusterService.getFileSystem();
		Path workFlowRootPath = new Path(workFlowRoot);
		fs.delete(workFlowRootPath, true);

		File wfDir = new ClassPathResource("oozie/bundle-hive-to-es-topqueries")
				.getFile();
		LOG.debug("wfdir: {}", wfDir.getAbsolutePath());
		FileUtil.copy(wfDir, fs, workFlowRootPath, false, new Configuration());
		FileUtil.copy(new ClassPathResource("hive/hive-site.xml").getFile(),
				fs, new Path(workFlowRoot), false, new Configuration());
		return workFlowRoot;
	}

	private String setupHiveAddPartitionWorkflowApp() throws IOException {
		String userName = System.getProperty("user.name");
		String workFlowRoot = hadoopClusterService.getHDFSUri()
				+ "/usr/" + userName + "/oozie/wf-hive-add-partition";

		// put oozie app in hadoop
		DistributedFileSystem fs = hadoopClusterService.getFileSystem();
		Path workFlowRootPath = new Path(workFlowRoot);
		fs.delete(workFlowRootPath, true);

		File wfDir = new ClassPathResource("oozie/wf-hive-add-partition")
				.getFile();
		LOG.debug("wfdir: {}", wfDir.getAbsolutePath());
		FileUtil.copy(wfDir, fs, workFlowRootPath, false, new Configuration());
		FileUtil.copy(new ClassPathResource("hive/hive-site.xml").getFile(),
				fs, new Path(workFlowRoot), false, new Configuration());
		return workFlowRoot;
	}
	
	private void submitTopQueriesBundleCoordJob(String workFlowRoot)
			throws OozieClientException, InterruptedException {
		// OozieClient client = LocalOozie.getCoordClient();
		String oozieURL = System.getProperty("oozie.base.url");
		LOG.debug("Oozie BaseURL is: {} ", oozieURL);
		OozieClient client = new OozieClient(oozieURL);
		Properties conf = client.createConfiguration();
		conf.setProperty(OozieClient.BUNDLE_APP_PATH, workFlowRoot
				+ "/load-and-index-customerqueries-bundle-configuration.xml");
		conf.setProperty("coordAppPathLoadCustomerQueries", workFlowRoot
				+ "/coord-app-load-customerqueries.xml");
		conf.setProperty("coordAppPathIndexTopQueriesES", workFlowRoot
				+ "/coord-app-index-topqueries-es.xml");
		
		conf.setProperty("nameNode", hadoopClusterService.getHDFSUri());
		conf.setProperty("jobTracker", hadoopClusterService.getJobTRackerUri());
		conf.setProperty("workflowRoot", workFlowRoot);
		String userName = System.getProperty("user.name");
		String oozieWorkFlowRoot = hadoopClusterService.getHDFSUri()
				+ "/usr/" + userName +"/oozie";
		conf.setProperty("oozieWorkflowRoot", oozieWorkFlowRoot);
		Date now = new Date();
		conf.setProperty("jobStart", DateUtils.formatDateOozieTZ(now));
		conf.setProperty("jobStartIndex", DateUtils.formatDateOozieTZ(new DateTime(now).plusMinutes(1).toDate()));
		conf.setProperty("jobEnd", DateUtils.formatDateOozieTZ(new DateTime()
				.plusDays(2).toDate()));
		conf.setProperty("initialDataset", DateUtils.formatDateOozieTZ(now));
		conf.setProperty("tzOffset", "1");

		// submit and start the workflow job
		String jobId = client.submit(conf);

		LOG.debug("Workflow job submitted");
		// wait until the workflow job finishes printing the status every 10
		// seconds
		int retries = 3;
		for (int i = 1; i <= retries; i++) {
			// Sleep 60 sec./ 3 mins
			Thread.sleep(60 * 1000);
			
			BundleJob bundleJobInfo = client.getBundleJobInfo(jobId);
			LOG.debug("Workflow job running ...");
			LOG.debug("bundleJobInfo Try: {}", i);
			LOG.debug("bundleJobInfo StartTime: {}", bundleJobInfo.getStartTime());
			LOG.debug("bundleJobInfo EndTime: {}", bundleJobInfo.getEndTime());
			LOG.debug("bundleJobInfo ConsoleURL: {}",
					bundleJobInfo.getConsoleUrl());
			LOG.debug("bundleJobInfo Status: {}", bundleJobInfo.getStatus());

			for (CoordinatorJob coordinatorJob : bundleJobInfo.getCoordinators()) {
				LOG.debug("bundleJobInfo StartTime: {}", coordinatorJob.getStartTime());
				LOG.debug("bundleJobInfo EndTime: {}", coordinatorJob.getEndTime());
				LOG.debug("coordJobInfo NextMaterizedTime: {}",
						coordinatorJob.getNextMaterializedTime());
				LOG.debug("bundleJobInfo Frequency: {}", coordinatorJob.getFrequency());
//				LOG.debug("bundleJobInfo ActionConsoleURL: {}", coordinatorJob
//						.getActions().get(0).getConsoleUrl());
//				LOG.debug("coordJobInfo ActionErrorMessage: {}", coordinatorJob
//						.getActions().get(0).getErrorMessage());
			};
			if(bundleJobInfo.getStatus() == Job.Status.RUNNING)
			{
				//Wait three times to see the running state is stable..then it is fine.
				//Job will keep running even if hive action fails.
				if(i == 3)
				{
					LOG.info("Coord Job in running state! " + bundleJobInfo.getStatus());
					break;
				}
				else
				{
					continue;
				}
			}
			else if(bundleJobInfo.getStatus() == Job.Status.PREMATER || bundleJobInfo.getStatus() == Job.Status.PREP)
			{
				//still preparing.
				continue;
			}else
			{
				throw new RuntimeException("Error occured while running customer top queries bundle job! " + bundleJobInfo.getStatus());
			}
		}
	}

	private void submitCoordJob(String workFlowRoot)
			throws OozieClientException, InterruptedException {
		// OozieClient client = LocalOozie.getCoordClient();
		String oozieURL = System.getProperty("oozie.base.url");
		LOG.debug("Oozie BaseURL is: {} ", oozieURL);
		OozieClient client = new OozieClient(oozieURL);
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
		String jobId = client.submit(conf);

		LOG.debug("Workflow job submitted");
		// wait until the workflow job finishes printing the status every 10
		// seconds
		int retries = 3;
		for (int i = 1; i <= retries; i++) {
			// Sleep 60 sec./ 3 mins
			Thread.sleep(60 * 1000);
			
			CoordinatorJob coordJobInfo = client.getCoordJobInfo(jobId);
			LOG.debug("Workflow job running ...");
			LOG.debug("coordJobInfo Try: {}", i);
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
			if(coordJobInfo.getStatus() == Job.Status.RUNNING)
			{
				//Wait three times to see the running state is stable..then it is fine.
				//Job will keep running even if hive action fails.
				if(i == 3)
				{
					LOG.info("Coord Job in running state!");
					break;
				}
				else
				{
					continue;
				}
			}
			else if(coordJobInfo.getStatus() == Job.Status.PREMATER || coordJobInfo.getStatus() == Job.Status.PREP)
			{
				//still preparing.
				continue;
			}else
			{
				throw new RuntimeException("Error occured while running coord job!");
			}
		}
	}

	private void submitWorkflowJob(String workFlowRoot)
			throws OozieClientException, InterruptedException {
		String oozieURL = System.getProperty("oozie.base.url");
		LOG.debug("Oozie BaseURL is: {} ", oozieURL);
		OozieClient client = new OozieClient(oozieURL);

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

		Properties conf = client.createConfiguration();
		conf.setProperty(OozieClient.APP_PATH, workFlowRoot
				+ "/hive-action-add-partition.xml");
		conf.setProperty("nameNode", hadoopClusterService.getHDFSUri());
		conf.setProperty("jobTracker", hadoopClusterService.getJobTRackerUri());
		conf.setProperty("workflowRoot", workFlowRoot);
		conf.setProperty("YEAR", year);
		conf.setProperty("MONTH", month);
		conf.setProperty("DAY", day);
		conf.setProperty("HOUR", hour);
		conf.setProperty("oozie.use.system.libpath", "true");

		// submit and start the workflow job
		client.setDebugMode(1);
		// client.dryrun(conf);
		String jobId = client.run(conf);// submit(conf);

		LOG.debug("Workflow job submitted");
		// wait until the workflow job finishes printing the status every 10
		// seconds
		int retries = 3;
		for (int i = 1; i <= retries; i++) {
			// Sleep 60 sec./ 3 mins
			Thread.sleep(60 * 1000);

			WorkflowJob jobInfo = client.getJobInfo(jobId);
			Status jobStatus = jobInfo.getStatus();
			LOG.debug("Workflow job running ...");
			LOG.debug("HiveActionWorkflowJob Status Try: {}", i);
			LOG.debug("HiveActionWorkflowJob Id: {}", jobInfo.getId());
			LOG.debug("HiveActionWorkflowJob StartTime: {}",
					jobInfo.getStartTime());
			LOG.debug("HiveActionWorkflowJob EndTime: {}", jobInfo.getEndTime());
			LOG.debug("HiveActionWorkflowJob ConsoleURL: {}",
					jobInfo.getConsoleUrl());
			LOG.debug("HiveActionWorkflowJob Status: {}", jobInfo.getStatus());

			WorkflowAction workflowAction = jobInfo.getActions().get(0);

			LOG.debug("HiveActionWorkflowJob Action consoleURL: {}",
					workflowAction.getConsoleUrl());
			LOG.debug("HiveActionWorkflowJob Action Name: {}",
					workflowAction.getName());
			LOG.debug("HiveActionWorkflowJob Action error message: {}",
					workflowAction.getErrorMessage());
			LOG.debug("HiveActionWorkflowJob Action Status: {}",
					workflowAction.getStats());
			LOG.debug("HiveActionWorkflowJob Action data: {}",
					workflowAction.getData());
			LOG.debug("HiveActionWorkflowJob Action conf: {}",
					workflowAction.getConf());
			LOG.debug("HiveActionWorkflowJob Action retries: {}",
					workflowAction.getRetries());
			LOG.debug("HiveActionWorkflowJob Action id: {}",
					workflowAction.getId());
			LOG.debug("HiveActionWorkflowJob Action start time: {}",
					workflowAction.getStartTime());
			LOG.debug("HiveActionWorkflowJob Action end time: {}",
					workflowAction.getEndTime());
			LOG.debug("HiveActionWorkflowJob Oozie Url: {}",
					client.getOozieUrl());

			if (jobStatus == WorkflowJob.Status.SUCCEEDED) {
				LOG.info("Oozie workflow job was successful!" + jobStatus);
				break;
			} else if (jobStatus == WorkflowJob.Status.PREP
					|| jobStatus == WorkflowJob.Status.RUNNING) {
				if (i == retries) {
					throw new RuntimeException("Error executing workflow job!"
							+ jobStatus);
				} else {
					continue;
				}
			} else {
				throw new RuntimeException("Error executing workflow job!"
						+ jobStatus);
			}
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
			// Fully delete dir
			FileUtils.deleteDirectory(oozieHome);
			oozieHome.mkdir();
			File oozieConf = new File(oozieHome, "conf");
			oozieConf.mkdir();
			FileUtils.copyFileToDirectory(new ClassPathResource(
					"oozie/oozie-site.xml").getFile(), oozieConf);
			File oozieHadoopConf = new File(oozieConf, "hadoop-conf");
			oozieHadoopConf.mkdir();
			FileUtils.copyFileToDirectory(new ClassPathResource(
					"core-site.xml").getFile(), oozieHadoopConf);
			FileUtils.copyFileToDirectory(new ClassPathResource(
					"mapred-site.xml").getFile(), oozieHadoopConf);
			File oozieActionConf = new File(oozieConf, "action-conf");
			oozieActionConf.mkdir();
			FileUtils.copyFileToDirectory(new ClassPathResource(
					"hive/hive-site.xml").getFile(), oozieActionConf);
			oozieData.mkdir();

			String userName = System.getProperty("user.name");
			// FS set up
			String sharedLibPathString = "/usr/" + userName +"/share/lib/lib_20140414170412/";
			Path sharedLibPath = new Path(sharedLibPathString);
			DistributedFileSystem fs = hadoopClusterService.getFileSystem();
			fs.mkdirs(sharedLibPath);
			Path ooziePath = new Path(sharedLibPath + "/oozie");
			fs.mkdirs(ooziePath);
			Path hivePath = new Path(sharedLibPath + "/hive");
			fs.mkdirs(hivePath);

			for (String oozieJar : listAllOozieLibs()) {
				LOG.debug("Adding jar file to shared lib: {}", oozieJar);
				if (oozieJar.contains("hive")) {
					FileUtil.copy(new File(oozieJar), fs, hivePath, false,
							new Configuration());
				} else {
					FileUtil.copy(new File(oozieJar), fs, ooziePath, false,
							new Configuration());
				}
			}

		} catch (IOException e) {
			String errMsg = "Error setting up oozie home!";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}

		// Start local oozie.
		try {

			System.setProperty("oozie.data.dir", oozieData.getAbsolutePath());
			System.setProperty("oozie.log.dir",
					new File(oozieHome, "logs").getAbsolutePath());
			System.setProperty(XLogService.LOG4J_FILE, "log4j.properties");

			JaiLocalOozie.start();
			String callbackBaseUrl = Services.get().getConf()
					.get(CallbackService.CONF_BASE_URL);
			LOG.debug("Callback base url is: {}", callbackBaseUrl);
			String baseURL = callbackBaseUrl.substring(0,
					callbackBaseUrl.lastIndexOf("/"));
			Services.get().getConf().set("oozie.base.url", baseURL);
			System.setProperty("oozie.base.url", baseURL);
		} catch (Exception e) {
			e.printStackTrace();
			String errMsg = "Error occured while starting local oozie!";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
	}

	private List<String> listAllOozieLibs() {
		List<String> oozieJars = new ArrayList<>();
		final String pathSep = System.getProperty("path.separator");
		final String list = System.getProperty("java.class.path");
		for (final String path : list.split(pathSep)) {
			final File file = new java.io.File(path);
			if (file.isDirectory())
				listDirectory(file, oozieJars);
			else if (file.getName().endsWith(".jar")
					&& (file.getName().contains("oozie") || file.getName()
							.contains("hadoop"))) {
				oozieJars.add(file.getAbsolutePath());
			}
		}
		return oozieJars;
	}

	private void listDirectory(File fileDir, List<String> oozieJars) {
		File list[] = fileDir.listFiles();
		for (File file : list) {
			if (file.isDirectory())
				listDirectory(file, oozieJars);
			else if (file.getName().endsWith(".jar")
					&& (file.getName().contains("oozie") || file.getName()
							.contains("oozie"))) {
				oozieJars.add(file.getAbsolutePath());
			}
		}
	}

	@Override
	public void shutdown() {
		JaiLocalOozie.stop();
	}

}
