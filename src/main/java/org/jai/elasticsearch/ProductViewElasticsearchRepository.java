package org.jai.elasticsearch;

import org.springframework.data.elasticsearch.repository.ElasticsearchCrudRepository;

public interface ProductViewElasticsearchRepository extends
		ElasticsearchCrudRepository<ProductView, String> {

}
