package org.jai.search.analytics;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;

import java.net.UnknownHostException;
import java.util.List;

public interface GenerateSearchAnalyticsDataService
{
    void generateAndPushSearchEvents(int numberOfEvents) throws UnknownHostException, JsonProcessingException, EventDeliveryException,
            InterruptedException;
    
    List<Event> getSearchEvents(int numberOfEvents) throws UnknownHostException, JsonProcessingException, EventDeliveryException,
    InterruptedException;
}
