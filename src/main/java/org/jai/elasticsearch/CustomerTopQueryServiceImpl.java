package org.jai.elasticsearch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CustomerTopQueryServiceImpl implements CustomerTopQueryService {

	private static final Logger LOG = LoggerFactory
			.getLogger(CustomerTopQueryServiceImpl.class);
	@Autowired
	private TopQueryElasticsearchRepository topQueryElasticsearchRepository;

	@Override
	public long countTotalRecords() {
		long count = topQueryElasticsearchRepository.count();
		LOG.debug("Total count returned for TopQueryRepository is: {}", count);
		return count;
	}

}
