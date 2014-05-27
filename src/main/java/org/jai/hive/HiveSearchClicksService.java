package org.jai.hive;

import java.util.List;

public interface HiveSearchClicksService {

	List<String> getDbs();

	List<String> getTables(String dnName);

	void setup();

	int getTotalRowCount(String dbName, String tbName);

	void addPartition(String dbName, String tbName, String year, String month,
			String day, String hour);
	
	void printHivePartitions(String dbName, String tbName);

	void getSearchClicks(String dbName, String tbName, String year,
			String month, String day, String hour);

	void loadSearchCustomerQueryTable();

	void loadTopSearchCustomerQueryToElasticSearchIndex();

	void loadProductViewsTable();

	void loadProductViewsToElasticSearchIndex();
}
