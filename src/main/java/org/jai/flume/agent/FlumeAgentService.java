package org.jai.flume.agent;

import org.apache.flume.agent.embedded.EmbeddedAgent;

/**
 * Service is used to put search events to external flume agent. Currently
 * configured to put under rolling file sink.
 * 
 * @author Jai
 *
 */
public interface FlumeAgentService {

	void setup();

	void shutdown();

	EmbeddedAgent getFlumeAgent();

	void processAllEvents();
}
