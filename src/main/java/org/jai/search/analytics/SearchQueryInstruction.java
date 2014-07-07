package org.jai.search.analytics;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
public class SearchQueryInstruction implements Serializable {
	@JsonIgnore
	@JsonProperty(value = "eventidsuffix")
	private String _eventIdSuffix;

	@JsonProperty(value = "eventid", required = true)
	private String eventId;

	@JsonProperty(value = "hostedmachinename")
	private String hostedMachineName;

	@JsonProperty(value = "pageurl")
	private String pageUrl;

	@JsonProperty(value = "customerid")
	private Long customerId;

	@JsonProperty(value = "sessionid")
	private String sessionId;

	@JsonProperty(value = "querystring")
	private String queryString;

	@JsonProperty(value = "sortorder")
	private String sortOrder;

	@JsonProperty(value = "pagenumber")
	private Long pageNumber;

	@JsonProperty(value = "totalhits")
	private Long totalHits;

	@JsonProperty(value = "hitsshown")
	private Long hitsShown;

	@JsonProperty(value = "createdtimestampinmillis")
	private final Long createdTimeStampInMillis;

	@JsonProperty(value = "clickeddocid")
	private String clickedDocId;

	private Boolean favourite;

	public SearchQueryInstruction() {
		_eventIdSuffix = UUID.randomUUID().toString();
		createdTimeStampInMillis = new Date().getTime();
	}

	@JsonIgnore
	private Map<String, Set<String>> filters;

	@JsonProperty(value = "filters")
	private List<FacetFilter> _filters;

	public String getEventId() {
		return eventId;
	}

	public String getEventIdSuffix() {
		return _eventIdSuffix;
	}

	public void setEventIdSuffix(String eventIdSuffix) {
		this._eventIdSuffix = eventIdSuffix;
	}

	public Long getCreatedTimeStampInMillis() {
		return createdTimeStampInMillis;
	}

	public void setEventId(final String eventId) {
		this.eventId = eventId;
	}

	public String getHostedMachineName() {
		return hostedMachineName;
	}

	public void setHostedMachineName(final String hostedMachineName) {
		this.hostedMachineName = hostedMachineName;
	}

	public String getPageUrl() {
		return pageUrl;
	}

	public void setPageUrl(final String pageUrl) {
		this.pageUrl = pageUrl;
	}

	public Long getCustomerId() {
		return customerId;
	}

	public void setCustomerId(final Long customerId) {
		this.customerId = customerId;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(final String sessionId) {
		this.sessionId = sessionId;
	}

	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(final String queryString) {
		this.queryString = queryString;
	}

	public String getSortOrder() {
		return sortOrder;
	}

	public void setSortOrder(final String sortOrder) {
		this.sortOrder = sortOrder;
	}

	public Long getPageNumber() {
		return pageNumber;
	}

	public void setPageNumber(final Long pageNumber) {
		this.pageNumber = pageNumber;
	}

	public Long getTotalHits() {
		return totalHits;
	}

	public void setTotalHits(final Long totalHits) {
		this.totalHits = totalHits;
	}

	public Long getHitsShown() {
		return hitsShown;
	}

	public void setHitsShown(final Long hitsShown) {
		this.hitsShown = hitsShown;
	}

	public String getClickedDocId() {
		return clickedDocId;
	}

	public void setClickedDocId(final String clickedDocId) {
		this.clickedDocId = clickedDocId;
	}

	public Boolean getFavourite() {
		return favourite;
	}

	public void setFavourite(final Boolean favourite) {
		this.favourite = favourite;
	}

	public Map<String, Set<String>> getFilters() {
		return filters;
	}
	
	@JsonIgnore
	public List<FacetFilter> getFacetFilters() {
		return _filters;
	}

	public void setFilters(final Map<String, Set<String>> newFilters) {
		if (filters == null) {
			filters = new HashMap<>();
		} else {
			filters.clear();
		}

		if (_filters == null) {
			_filters = new ArrayList<SearchQueryInstruction.FacetFilter>();
		} else {
			_filters.clear();
		}

		for (final Entry<String, Set<String>> entry : newFilters.entrySet()) {
			Set<String> filter = filters.get(entry.getKey());
			if (filter == null) {
				filter = new HashSet<>();
			}
			for (final String filterValue : entry.getValue()) {
				_filters.add(new FacetFilter(entry.getKey(), filterValue));
				filter.add(filterValue);
			}
			filters.put(entry.getKey(), filter);
		}
		// this.filters = newFilters;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append(hostedMachineName)
				.append(pageUrl).append(customerId).append(sessionId)
				.append(queryString).append(sortOrder).append(pageNumber)
				.append(totalHits).append(hitsShown)
				.append(createdTimeStampInMillis).append(clickedDocId)
				.append(filters).append(eventId).append(_filters)
				.append(_eventIdSuffix).toString();
	}

	public static class FacetFilter implements Serializable {
		private String code;
		private String value;

		public FacetFilter() {
		}

		public FacetFilter(String code, String value) {
			this.code = code;
			this.value = value;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}

		public String getCode() {
			return code;
		}

		public void setCode(String code) {
			this.code = code;
		}

		@Override
		public String toString() {
			return new ToStringBuilder(this).append(code).append(value)
					.toString();
		}
	}
}
