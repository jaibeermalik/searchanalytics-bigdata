package org.jai.flume.sinks.hbase;

import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.Sink;

public interface FlumeHbaseSinkService {
	Sink getSink();

	void processEvents(List<Event> events);

	void start();

	void shutdown();

	Channel getChannel();
}
