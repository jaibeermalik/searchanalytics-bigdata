package org.jai.elasticsearch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ElasticSearchRepoServiceImpl implements ElasticSearchRepoService {

	private static final Logger LOG = LoggerFactory
			.getLogger(ElasticSearchRepoServiceImpl.class);
	@Autowired
	private TopQueryElasticsearchRepository topQueryElasticsearchRepository;
	@Autowired
	private ProductViewElasticsearchRepository productViewElasticsearchRepository;

	@Override
	public long countCustomerTopQueryTotalRecords() {
		long count = topQueryElasticsearchRepository.count();
		LOG.debug("Total count returned for TopQueryRepository is: {}", count);
		return count;
	}
	
	@Override
	public void deleteAllCustomerTopQueryRecords() {
		topQueryElasticsearchRepository.deleteAll();
	}
	@Override
	public long countProductViewsTotalRecords() {
		long count = productViewElasticsearchRepository.count();
		LOG.debug("Total count returned for productViewElasticsearchRepository is: {}", count);
		return count;
	}
	
	@Override
	public void deleteAllProductViewsRecords() {
		productViewElasticsearchRepository.deleteAll();
	}

}
