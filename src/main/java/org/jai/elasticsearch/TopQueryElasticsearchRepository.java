package org.jai.elasticsearch;

import org.springframework.data.elasticsearch.repository.ElasticsearchCrudRepository;

public interface TopQueryElasticsearchRepository extends
		ElasticsearchCrudRepository<CustomerTopQuery, String> {

}
