package org.jai.hadoop.hdfs;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.springframework.stereotype.Service;

@Service
public class HadoopClusterServiceImpl implements HadoopClusterService {

	private MiniDFSCluster miniDFSCluster;
	
	@Override
	public DistributedFileSystem getFileSystem() {
		try {
			return miniDFSCluster.getFileSystem();
		} catch (IOException e) {
			throw new RuntimeException("Error accessing file system!",e);
		}
	}

	@Override
	public String getHDFSUri() {
		String hdfsURI = "hdfs://localhost.localdomain:"
				+ miniDFSCluster.getNameNodePort();
		return hdfsURI;
	}
	
	@Override
	public void start() {
		startHdfsCluster();
	}
	
	@Override
	public void shutdown() {
		miniDFSCluster.shutdown(true);
	}

	private void startHdfsCluster(){
		//already running
		if(miniDFSCluster !=null)
		{
			return;
		}
		
		String testName = "trial";
		File baseDir = new File("./target/hdfs/" + testName).getAbsoluteFile();
		FileUtil.fullyDelete(baseDir);
		Configuration conf = new Configuration();
		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		builder.nameNodePort(54321);
		try {
			miniDFSCluster = builder.build();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Error starting cluster!",e);
		}
		System.out.println("Mini Cluster started!" + getHDFSUri());
	}

}
