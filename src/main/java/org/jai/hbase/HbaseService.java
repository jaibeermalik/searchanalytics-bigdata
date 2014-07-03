package org.jai.hbase;

public interface HbaseService {

	void getSearchClicks();

	void testHbaseServer();
	
	void setup();
	void shutdown();

	void setupSearchEventsTable();

	void insertEventData(byte[] body);

	int getTotalSearchClicksCount();

	void removeAll();
}
