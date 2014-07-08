package org.jai.hbase;

import java.util.List;

public interface HbaseService {

	void getSearchClicks();

	void testHbaseServer();
	
	void setup();
	void shutdown();

	void setupSearchEventsTable();

	void insertEventData(byte[] body);

	int getTotalSearchClicksCount();

	void removeAll();

	int findTotalRecordsForValidCustomers();

	List<String> findTopTenSearchQueryStringForLastAnHour();
	
	List<String> findTopTenSearchFiltersForLastAnHour();
	
	List<String> findTopTenSearchFiltersForLastAnHourUsingRangeScan();
	
	int numberOfTimesAFacetFilterClickedInLastAnHour(final String columnName, final String columnValue);

	List<String> getAllSearchQueryStringsByCustomerInLastOneMonth(Long customerId);
}
