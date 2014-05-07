package org.jai.search.index;

import java.util.List;

import org.jai.search.config.ElasticSearchIndexConfig;
import org.jai.search.model.Product;
import org.jai.search.model.ProductGroup;
import org.jai.search.model.ProductProperty;

public interface IndexProductDataService {
	void indexAllProducts(ElasticSearchIndexConfig config,
			List<Product> products);

	void indexProduct(ElasticSearchIndexConfig config, String indexName,
			Product product);

	void indexProductPropterty(ElasticSearchIndexConfig config,
			String indexName, ProductProperty productProperty);

	boolean isProductExists(ElasticSearchIndexConfig config, Long productId);

	void deleteProduct(ElasticSearchIndexConfig config, Long productId);

	void indexProductGroup(ElasticSearchIndexConfig config, String indexName,
			ProductGroup productGroup);
}
