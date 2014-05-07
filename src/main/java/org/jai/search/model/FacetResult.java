package org.jai.search.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

public class FacetResult {
	private String code;

	private final List<FacetResultEntry> facetResultEntries = new ArrayList<FacetResultEntry>();

	public String getCode() {
		return code;
	}

	public void setCode(final String code) {
		this.code = code;
	}

	public List<FacetResultEntry> getFacetResultEntries() {
		return facetResultEntries;
	}

	public void addFacetResultEntry(final FacetResultEntry facetResultEntry) {
		facetResultEntries.add(facetResultEntry);
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append(code)
				.append(facetResultEntries).toString();
	}
}
