package org.jai.search.analytics;

import java.util.List;

import org.apache.flume.Event;

public interface GenerateSearchAnalyticsDataService
{
    void generateAndPushSearchEvents(int numberOfEvents);
    
    List<Event> getSearchEvents(int numberOfEvents);
}
