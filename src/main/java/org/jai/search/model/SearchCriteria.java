package org.jai.search.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.search.sort.SortOrder;

public class SearchCriteria {
	private String query;

	private String[] indices;

	private String[] documentTypes;

	private int from = 0;

	private int size = 10;

	private SortOrder sortOrder;

	private boolean noFacets;

	private final List<String> facets = new ArrayList<String>();

	// Used to handle single select in "and" operations
	private final Map<String, String> singleSelectFilters;

	// Used for "or" operations between different facet entries.
	private final Map<String, List<String>> multiSelectFilters;

	// Used to filter on field values separately
	// OR { condition1 AND {condition2 OR condition3 } }
	private final List<Map<String, Object>> fieldValueFilters;

	private final List<ProductProperty> productProperties;

	private final List<Specification> specifications;

	// sold out items at the end of list.
	private boolean rescoreOnSoldOut;

	private boolean useBoostingFactor;

	public SearchCriteria() {
		singleSelectFilters = new LinkedHashMap<String, String>();
		multiSelectFilters = new LinkedHashMap<String, List<String>>();
		fieldValueFilters = new ArrayList<Map<String, Object>>();
		productProperties = new ArrayList<ProductProperty>();
		specifications = new ArrayList<Specification>();
	}

	public String[] getIndexes() {
		return indices;
	}

	public SearchCriteria indices(final String... indices) {
		this.indices = indices;
		return this;
	}

	public String[] getDocumentTypes() {
		return documentTypes;
	}

	public SearchCriteria documentTypes(final String... documentTypes) {
		this.documentTypes = documentTypes;
		return this;
	}

	public SearchCriteria query(final String query) {
		this.query = query;
		return this;
	}

	public String getQuery() {
		return query;
	}

	public int getFrom() {
		return from;
	}

	public SearchCriteria from(final int from) {
		this.from = from;
		return this;
	}

	public SearchCriteria size(final int size) {
		this.size = size;
		return this;
	}

	public int getSize() {
		return size;
	}

	public SortOrder getSortOrder() {
		return sortOrder;
	}

	public SearchCriteria sortOrder(final SortOrder sortOrder) {
		this.sortOrder = sortOrder;
		return this;
	}

	public boolean isNoFacets() {
		return noFacets;
	}

	public SearchCriteria noFacets(final boolean noFacets) {
		this.noFacets = noFacets;
		return this;
	}

	public List<String> getFacets() {
		return facets;
	}

	public SearchCriteria facets(final String facet) {
		facets.add(facet);
		return this;
	}

	public void addFiledValueFilter(final String fieldName, final Object value) {
		final Map<String, Object> fieldCrtiteria = new HashMap<String, Object>();
		fieldCrtiteria.put(fieldName, value);
		fieldValueFilters.add(fieldCrtiteria);
	}

	public SearchCriteria addMultiSelectFilter(final String facetCode,
			final String facetTerm) {
		List<String> list = multiSelectFilters.get(facetCode);
		if (list == null) {
			list = new ArrayList<String>();
			multiSelectFilters.put(facetCode, list);
		}
		list.add(facetTerm);
		return this;
	}

	public SearchCriteria addSingleSelectFilter(final String facetCode,
			final String facetTerm) {
		singleSelectFilters.put(facetCode, facetTerm);
		return this;
	}

	public boolean hasFilters() {
		return singleSelectFilters.size() > 0 || multiSelectFilters.size() > 0
				|| fieldValueFilters.size() > 0 || productProperties.size() > 0
				|| specifications.size() > 0;
	}

	public List<Map<String, Object>> getFieldValueFilters() {
		return fieldValueFilters;
	}

	public Map<String, String> getSingleSelectFilters() {
		return singleSelectFilters;
	}

	public Map<String, List<String>> getMultiSelectFilters() {
		return multiSelectFilters;
	}

	public void addProductProperty(final ProductProperty productProperty) {
		productProperties.add(productProperty);
	}

	public List<ProductProperty> getProductProperties() {
		return productProperties;
	}

	public List<Specification> getSpecifications() {
		return specifications;
	}

	public void addSpecifications(final Specification specification) {
		specifications.add(specification);
	}

	public boolean isRescoreOnSoldOut() {
		return rescoreOnSoldOut;
	}

	public SearchCriteria rescoreOnSoldOut(final boolean rescoreOnSoldOut) {
		this.rescoreOnSoldOut = rescoreOnSoldOut;
		return this;
	}

	public boolean isUseBoostingFactor() {
		return useBoostingFactor;
	}

	public SearchCriteria useBoostingFactor(final boolean useBoostingFactor) {
		this.useBoostingFactor = useBoostingFactor;
		return this;
	}
}
