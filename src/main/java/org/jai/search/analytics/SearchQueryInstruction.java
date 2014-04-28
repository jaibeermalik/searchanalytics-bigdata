package org.jai.search.analytics;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

@SuppressWarnings("serial")
public class SearchQueryInstruction implements Serializable
{
    @JsonIgnore
    private final String _eventIdSuffix;

    private String eventId;

    private String hostedMachineName;

    private String pageUrl;

    private Long customerId;

    private String sessionId;

    private String queryString;

    private String sortOrder;

    private Long pageNumber;

    private Long totalHits;

    private Long hitsShown;

    private final Long createdTimeStampInMillis;

    private String clickedDocId;

    private Boolean favourite;

    public SearchQueryInstruction()
    {
        _eventIdSuffix = UUID.randomUUID().toString();
        createdTimeStampInMillis = new Date().getTime();
    }

    @JsonIgnore
    private Map<String, Set<String>> filters;

    @JsonProperty(value = "filters")
    private List<FacetFilter> _filters;

    public String getEventId()
    {
        return eventId;
    }

    public String getEventIdSuffix()
    {
        return _eventIdSuffix;
    }

    public Long getCreatedTimeStampInMillis()
    {
        return createdTimeStampInMillis;
    }

    public void setEventId(final String eventId)
    {
        this.eventId = eventId;
    }

    public String getHostedMachineName()
    {
        return hostedMachineName;
    }

    public void setHostedMachineName(final String hostedMachineName)
    {
        this.hostedMachineName = hostedMachineName;
    }

    public String getPageUrl()
    {
        return pageUrl;
    }

    public void setPageUrl(final String pageUrl)
    {
        this.pageUrl = pageUrl;
    }

    public Long getCustomerId()
    {
        return customerId;
    }

    public void setCustomerId(final Long customerId)
    {
        this.customerId = customerId;
    }

    public String getSessionId()
    {
        return sessionId;
    }

    public void setSessionId(final String sessionId)
    {
        this.sessionId = sessionId;
    }

    public String getQueryString()
    {
        return queryString;
    }

    public void setQueryString(final String queryString)
    {
        this.queryString = queryString;
    }

    public String getSortOrder()
    {
        return sortOrder;
    }

    public void setSortOrder(final String sortOrder)
    {
        this.sortOrder = sortOrder;
    }

    public Long getPageNumber()
    {
        return pageNumber;
    }

    public void setPageNumber(final Long pageNumber)
    {
        this.pageNumber = pageNumber;
    }

    public Long getTotalHits()
    {
        return totalHits;
    }

    public void setTotalHits(final Long totalHits)
    {
        this.totalHits = totalHits;
    }

    public Long getHitsShown()
    {
        return hitsShown;
    }

    public void setHitsShown(final Long hitsShown)
    {
        this.hitsShown = hitsShown;
    }

    public String getClickedDocId()
    {
        return clickedDocId;
    }

    public void setClickedDocId(final String clickedDocId)
    {
        this.clickedDocId = clickedDocId;
    }

    public Boolean getFavourite()
    {
        return favourite;
    }

    public void setFavourite(final Boolean favourite)
    {
        this.favourite = favourite;
    }

    public Map<String, Set<String>> getFilters()
    {
        return filters;
    }

    public void setFilters(final Map<String, Set<String>> filters)
    {
        if (_filters != null)
        {
            _filters.clear();
        }
        for (final Entry<String, Set<String>> entry : filters.entrySet())
        {
            for (final String filterValue : entry.getValue())
            {
                if (_filters == null)
                {
                    _filters = new ArrayList<SearchQueryInstruction.FacetFilter>();
                }
                _filters.add(new FacetFilter(entry.getKey(), filterValue));
            }
        }
        this.filters = filters;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this).append(hostedMachineName).append(pageUrl).append(customerId).append(sessionId).append(queryString)
                .append(sortOrder).append(pageNumber).append(totalHits).append(hitsShown).append(createdTimeStampInMillis)
                .append(clickedDocId).append(filters).toString();
    }

    @SuppressWarnings("unused")
    private static class FacetFilter implements Serializable
    {
        private String code;
        private String value;
        public FacetFilter(String code, String value)
        {
            this.code = code;
            this.value = value;
        }
        
        public String getValue()
        {
            return value;
        }
        public void setValue(String value)
        {
            this.value = value;
        }
        public String getCode()
        {
            return code;
        }
        public void setCode(String code)
        {
            this.code = code;
        }
    }
}
