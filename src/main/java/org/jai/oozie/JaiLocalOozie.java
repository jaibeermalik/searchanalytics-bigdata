package org.jai.oozie;

import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.CallbackService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.servlet.CallbackServlet;
import org.apache.oozie.servlet.V0JobServlet;
import org.apache.oozie.servlet.V1JobServlet;
import org.apache.oozie.servlet.V1JobsServlet;
import org.apache.oozie.servlet.V2JobServlet;
import org.apache.oozie.servlet.VersionServlet;
import org.apache.oozie.test.EmbeddedServletContainer;
import org.apache.oozie.util.XLog;

public class JaiLocalOozie extends LocalOozie {
	private static EmbeddedServletContainer container;
	private static boolean localOozieActive = false;

	public synchronized static void start() throws Exception {
		if (localOozieActive) {
			throw new IllegalStateException("LocalOozie is already initialized");
		}

		String log4jFile = System.getProperty(XLogService.LOG4J_FILE, null);
		String oozieLocalLog = System.getProperty("oozielocal.log", null);
		if (log4jFile == null) {
			System.setProperty(XLogService.LOG4J_FILE,
					"localoozie-log4j.properties");
		}
		if (oozieLocalLog == null) {
			System.setProperty("oozielocal.log", "./oozielocal.log");
		}

		localOozieActive = true;
		new Services().init();

		if (log4jFile != null) {
			System.setProperty(XLogService.LOG4J_FILE, log4jFile);
		} else {
			System.getProperties().remove(XLogService.LOG4J_FILE);
		}
		if (oozieLocalLog != null) {
			System.setProperty("oozielocal.log", oozieLocalLog);
		} else {
			System.getProperties().remove("oozielocal.log");
		}

		container = new EmbeddedServletContainer("oozie");
		container.addServletEndpoint("/callback", CallbackServlet.class);
		container.addServletEndpoint("/" + RestConstants.VERSIONS,
				VersionServlet.class);
		container.addServletEndpoint("/v0/job/*", V0JobServlet.class);
		container.addServletEndpoint("/v1/job/*", V1JobServlet.class);
		container.addServletEndpoint("/v2/job/*", V2JobServlet.class);
		container.addServletEndpoint("/v1/jobs", V1JobsServlet.class);
		container.addServletEndpoint("/v2/jobs", V1JobsServlet.class);
		// container.addServletEndpoint("/" + RestConstants.JOBS,
		// VersionServlet.class);
		// container.addServletEndpoint("/" + RestConstants.JOBS,
		// VersionServlet.class);
		// container.
		container.start();
		String callbackUrl = container.getServletURL("/callback");
		Services.get().getConf()
				.set(CallbackService.CONF_BASE_URL, callbackUrl);
		XLog.getLog(LocalOozie.class).info(
				"LocalOozie started callback set to [{0}]", callbackUrl);
	}

	public synchronized static void stop() {
		RuntimeException thrown = null;
		try {
			if (container != null) {
				container.stop();
			}
		} catch (RuntimeException ex) {
			thrown = ex;
		}
		container = null;
		XLog.getLog(LocalOozie.class).info("LocalOozie stopped");
		try {
			Services.get().destroy();
		} catch (RuntimeException ex) {
			if (thrown != null) {
				thrown = ex;
			}
		}
		localOozieActive = false;
		if (thrown != null) {
			throw thrown;
		}
	}

}
