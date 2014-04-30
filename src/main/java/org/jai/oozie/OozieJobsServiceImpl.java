package org.jai.oozie;

import java.io.File;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.Services;

public class OozieJobsServiceImpl {

	public OozieClient getClient()
	{
		return LocalOozie.getClient();
	}
	public void setup() {
		
		File oozieHome = new File("target/ooziehome");
		oozieHome.delete();
		oozieHome.mkdir();
		File oozieConf = new File(oozieHome,"conf");
		oozieConf.mkdir();
		File oozieHadoopConf = new File(oozieConf,"hadoop-conf");
		oozieHadoopConf.mkdir();
		File oozieActionConf = new File(oozieConf,"action-conf");
		oozieActionConf.mkdir();
		File oozieData = new File(oozieHome,"data");
		oozieData.mkdir();
		System.setProperty(Services.OOZIE_HOME_DIR, oozieHome.getAbsolutePath());
		System.setProperty("oozie.data.dir", oozieData.getAbsolutePath());
		System.setProperty("oozie.service.JPAService.jdbc.driver", "org.apache.derby.jdbc.EmbeddedDriver");
		System.setProperty("oozie.service.JPAService.jdbc.url", "jdbc:derby:${oozie.data.dir}/oozie-db;create=true");
		System.setProperty("oozie.service.JPAService.jdbc.username", "sa");
		System.setProperty("oozie.service.JPAService.jdbc.password", "");
		System.setProperty("oozie.service.JPAService.create.db.schema", "true");
		try {
			LocalOozie.start();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Error occured while starting local oozie!",e);
		}
	}

	
}
