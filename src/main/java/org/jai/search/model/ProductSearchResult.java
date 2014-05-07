package org.jai.search.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

public class ProductSearchResult {
	private long totalCount;

	private List<Product> products = new ArrayList<Product>();

	private final List<FacetResult> facets = new ArrayList<FacetResult>();

	public long getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(final long totalCount) {
		this.totalCount = totalCount;
	}

	public List<Product> getProducts() {
		return products;
	}

	public void setProducts(final List<Product> products) {
		this.products = products;
	}

	public void addProduct(final Product product) {
		products.add(product);
	}

	public List<FacetResult> getFacets() {
		return facets;
	}

	public void addFacet(final FacetResult facet) {
		facets.add(facet);
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append(totalCount).append(products)
				.append(facets).toString();
	}
}
