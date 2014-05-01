package org.jai.hadoop.hdfs;

import org.apache.hadoop.hdfs.DistributedFileSystem;

public interface HadoopClusterService {

	DistributedFileSystem getFileSystem();
	
	String getHDFSUri();
	String getJobTRackerUri();

	void start();
	void shutdown();

}
