package org.jai.flume.sinks.elasticsearch;

import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.Sink;

public interface FlumeESSinkService {

	Sink getSink();
	
	void processEvents(List<Event> events);

	void start();

	void shutdown();

	Channel getChannel();
}
