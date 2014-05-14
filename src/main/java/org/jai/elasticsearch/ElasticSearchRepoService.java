package org.jai.elasticsearch;

public interface ElasticSearchRepoService {

	long countCustomerTopQueryTotalRecords();

	void deleteAllCustomerTopQueryRecords();
	
	long countProductViewsTotalRecords();

	void deleteAllProductViewsRecords();
}
