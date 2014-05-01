package org.jai.oozie;


public interface OozieJobsService {

	void setup();
	
	void shutdown();

	void startHiveAddPartitionCoordJob();
}
