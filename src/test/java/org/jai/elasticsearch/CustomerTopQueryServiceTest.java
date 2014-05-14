package org.jai.elasticsearch;

import static org.junit.Assert.assertEquals;

import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class CustomerTopQueryServiceTest extends
		AbstractSearchJUnit4SpringContextTests {

	@Autowired
	private ElasticSearchRepoService customerTopQueryService;
	@Autowired
	private TopQueryElasticsearchRepository topQueryElasticsearchRepository;

	@Test
	public void testCountTotalRecords() {
		CustomerTopQuery entity = new CustomerTopQuery();
		entity.setId("blahblah");
		entity.setCustomerId(12345l);
		entity.setQueryString("queryString");
		entity.setCount(123);

		topQueryElasticsearchRepository.save(entity);
		assertEquals(1, customerTopQueryService.countCustomerTopQueryTotalRecords());
		topQueryElasticsearchRepository.delete(entity);
		assertEquals(0, customerTopQueryService.countCustomerTopQueryTotalRecords());
	}

}
