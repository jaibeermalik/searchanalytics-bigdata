package org.jai.hadoop.hdfs;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.springframework.stereotype.Service;

@Service
public class HadoopClusterServiceImpl implements HadoopClusterService {

	private MiniDFSCluster miniDFSCluster;
	private MiniMRCluster miniMRCluster;

	@Override
	public DistributedFileSystem getFileSystem() {
		try {
			return miniDFSCluster.getFileSystem();
		} catch (IOException e) {
			throw new RuntimeException("Error accessing file system!", e);
		}
	}

	@Override
	public String getHDFSUri() {
		String hdfsURI = "hdfs://localhost.localdomain:"
				+ miniDFSCluster.getNameNodePort();
		return hdfsURI;
	}
	
	@Override
	public String getJobTRackerUri() {
		return "localhost.localdomain:" + miniMRCluster.getJobTrackerPort();
	}

	@Override
	public void start() {
		startHdfsCluster();
		startMRCluster();
	}

	@Override
	public void shutdown() {
		miniMRCluster.shutdown();
		miniDFSCluster.shutdown(true);
	}

	private void startHdfsCluster() {
		// already running
		if (miniDFSCluster != null) {
			return;
		}

		String testName = "trial";
		File baseDir = new File("target/hdfs/" + testName)
				.getAbsoluteFile();
		FileUtil.fullyDelete(baseDir);
		Configuration configuration = new Configuration();
		configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
		configuration.set("hadoop.log.dir", new File("target/logs").getAbsolutePath());
		configuration.set("hadoop.tmp.dir", new File("target/hadooptmp").getAbsolutePath());
		configuration.set("hadoop.proxyuser.oozie.hosts", "*");
		configuration.set("hadoop.proxyuser.oozie.groups", "*");
		configuration.set("hadoop.proxyuser.tom.hosts", "*");
		configuration.set("hadoop.proxyuser.tom.groups", "*");
		configuration.set("hadoop.proxyuser.mapred.hosts", "*");
		configuration.set("hadoop.proxyuser.mapred.groups", "*");
		
		System.setProperty("hadoop.log.dir", new File("target/logs").getAbsolutePath());
		System.setProperty("hadoop.tmp.dir", new File("target/hadooptmp").getAbsolutePath());
		
//		configuration.set("mapred.local.dir", new File("target/mapreddir").getAbsolutePath());
//		configuration.set("dfs.datanode.hostname", "localhost.localdomain");
//		configuration.set("mapred.job.tracker", "localhost.localdomain:54310");
//		configuration.set("mapreduce.jobhistory.address", "localhost.localdomain:54311");
//		configuration.set("mapreduce.jobhistory.webapp.address", "localhost.localdomain:54312");
//		configuration.set("mapred.job.tracker.http.address", "localhost.localdomain:54313");
//		configuration.set("mapred.task.tracker.http.address", "localhost.localdomain:54314");
//		configuration.set("mapred.task.tracker.http.address", "localhost.localdomain:54314");
//		configuration.set("mapreduce.task.tmp.dir", new File("target/mapredtaskdir").getAbsolutePath());
//		configuration.set("hadoop.job.history.location", new File("target/logs/history").getAbsolutePath());
//		configuration.set("hadoop.log.dir", new File("target/logs").getAbsolutePath());
//		
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(configuration);
		builder.nameNodePort(54321);
		try {
			miniDFSCluster = builder.build();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Error starting cluster!", e);
		}
		System.out.println("Mini Cluster started!" + getHDFSUri());
	}

	private void startMRCluster() {
		// already running
		if (miniMRCluster != null) {
			return;
		}
		Configuration configuration = new Configuration();
		configuration.set("mapred.local.dir", new File("target/mapreddir").getAbsolutePath());
		configuration.set("dfs.datanode.hostname", "localhost.localdomain");
		configuration.set("mapred.job.tracker", "localhost.localdomain:54310");
		configuration.set("mapreduce.jobhistory.address", "localhost.localdomain:54311");
		configuration.set("mapreduce.jobhistory.webapp.address", "localhost.localdomain:54312");
		configuration.set("mapred.job.tracker.http.address", "localhost.localdomain:54313");
		configuration.set("mapred.task.tracker.http.address", "localhost.localdomain:54314");
		configuration.set("mapred.task.tracker.http.address", "localhost.localdomain:54314");
		configuration.set("mapreduce.task.tmp.dir", new File("target/mapredtaskdir").getAbsolutePath());
//		configuration.set("hadoop.job.history.location", "none");
		configuration.set("hadoop.log.dir", new File("target/logs").getAbsolutePath());
		
		//Job history won't start NPE
		System.setProperty("hadoop.log.dir", new File("target/logs").getAbsolutePath());
		System.setProperty("mapred.local.dir", new File("target/mapreddir").getAbsolutePath());
		System.setProperty("dfs.datanode.hostname", "localhost.localdomain");
		System.setProperty("mapred.job.tracker", "localhost.localdomain:54310");
		System.setProperty("mapreduce.jobhistory.address", "localhost.localdomain:54311");
		System.setProperty("mapreduce.jobhistory.webapp.address", "localhost.localdomain:54312");
		System.setProperty("mapred.job.tracker.http.address", "localhost.localdomain:54313");
		System.setProperty("mapred.task.tracker.http.address", "localhost.localdomain:54314");
		System.setProperty("mapred.task.tracker.http.address", "localhost.localdomain:54314");
		System.setProperty("mapreduce.task.tmp.dir", new File("target/mapredtaskdir").getAbsolutePath());
		System.setProperty("hadoop.job.history.location", "none");
		System.setProperty("hadoop.log.dir", new File("target/logs").getAbsolutePath());
		 
		try {
			DistributedFileSystem fileSystem = miniDFSCluster.getFileSystem();
//			miniMRCluster = new MiniMRCluster(1, fileSystem.getUri()
//					.toString(), 1, null, null, new JobConf(configuration));
			miniMRCluster = new MiniMRCluster(54310, 54311, 1, fileSystem.getUri()
					.toString(), 1);
//			System.setProperty("mapred.job.tracker", miniMRCluster.createJobConf(new JobConf(configuration)).get("mapred.job.tracker"));
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Error starting cluster!", e);
		}

		System.out.println("Mini Cluster started!" + getHDFSUri());
	}

}
