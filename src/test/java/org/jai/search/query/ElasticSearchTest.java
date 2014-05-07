package org.jai.search.query;

import static org.junit.Assert.assertEquals;

import org.jai.search.config.ElasticSearchIndexConfig;
import org.jai.search.model.Product;
import org.jai.search.model.ProductSearchResult;
import org.jai.search.model.SearchCriteria;
import org.jai.search.test.AbstractSearchJUnit4SpringContextTests;
import org.junit.Test;

public class ElasticSearchTest extends AbstractSearchJUnit4SpringContextTests {
	@Test
	public void paginatedDocumentResults() {
		final ElasticSearchIndexConfig config = ElasticSearchIndexConfig.COM_WEBSITE;
		final SearchCriteria searchCriteria = new SearchCriteria();
		searchCriteria.indices(config.getIndexAliasName());
		searchCriteria.documentTypes(config.getDocumentType());
		searchCriteria.size(0);
		final ProductSearchResult searchProducts = productQueryService
				.searchProducts(searchCriteria);
		assertEquals(50, searchProducts.getTotalCount());
		assertEquals(0, searchProducts.getProducts().size());
	}

	@Test
	public void searchInMultipleIndexes() {
		final ElasticSearchIndexConfig config = ElasticSearchIndexConfig.COM_WEBSITE;
		final SearchCriteria searchCriteria = new SearchCriteria();
		searchCriteria.indices(config.getIndexAliasName(),
				ElasticSearchIndexConfig.NL_WEBSITE.getIndexAliasName());
		searchCriteria.documentTypes(config.getDocumentType());
		searchCriteria.size(0);
		final ProductSearchResult searchProducts = productQueryService
				.searchProducts(searchCriteria);
		// 50 + 50 docs from both indices
		assertEquals(100, searchProducts.getTotalCount());
		assertEquals(0, searchProducts.getProducts().size());
	}

	@Test
	public void SearchDocumentReturnedFileds() {
		final ElasticSearchIndexConfig config = ElasticSearchIndexConfig.COM_WEBSITE;
		final SearchCriteria searchCriteria = new SearchCriteria();
		searchCriteria.indices(config.getIndexAliasName());
		searchCriteria.documentTypes(config.getDocumentType());
		final ProductSearchResult searchProducts = productQueryService
				.searchProducts(searchCriteria);
		// 50, returned based on boosting.
		assertEquals(50, searchProducts.getTotalCount());
		assertEquals(10, searchProducts.getProducts().size());
		for (final Product product : searchProducts.getProducts()) {
			assertEquals("Title " + product.getId(), product.getTitle());
			assertEquals(product.getId().floatValue(), product.getPrice()
					.floatValue(), 0);
		}
	}

	@Test
	public void queryText() {
		final ElasticSearchIndexConfig config = ElasticSearchIndexConfig.COM_WEBSITE;
		final SearchCriteria searchCriteria = new SearchCriteria();
		searchCriteria.indices(config.getIndexAliasName());
		searchCriteria.documentTypes(config.getDocumentType());
		searchCriteria.query("query");
		ProductSearchResult searchProducts = productQueryService
				.searchProducts(searchCriteria);
		// 50 + 50 docs from both indices
		assertEquals(0, searchProducts.getTotalCount());
		assertEquals(0, searchProducts.getProducts().size());
		searchCriteria.query("Title");
		searchProducts = productQueryService.searchProducts(searchCriteria);
		// 50 + 50 docs from both indices
		assertEquals(50, searchProducts.getTotalCount());
		assertEquals(10, searchProducts.getProducts().size());
		searchCriteria.query("tile*");
		searchProducts = productQueryService.searchProducts(searchCriteria);
		// 0, special characters are escaped out
		assertEquals(0, searchProducts.getTotalCount());
		assertEquals(0, searchProducts.getProducts().size());
	}

}
