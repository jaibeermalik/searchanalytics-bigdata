package org.jai.flume.agent;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.flume.agent.embedded.EmbeddedAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class FlumeAgentServiceImpl implements FlumeAgentService{

	private static final Logger LOG = LoggerFactory.getLogger(FlumeAgentServiceImpl.class);
	
	private static EmbeddedAgent agent;
	
	@PostConstruct
    public void setupAgent()
    {
        createAgent();
    }

    @PreDestroy
    public void setupAfter()
    {
        if (agent != null)
        {
            agent.stop();
        }
    }
    
    private void createAgent()
    {
        final Map<String, String> properties = new HashMap<String, String>();
        properties.put("channel.type", "memory");
        properties.put("channel.capacity", "200");
        // a1.channels.c1.type = file
        // a1.channels.c1.checkpointDir = /mnt/flume/checkpoint
        // a1.channels.c1.dataDirs = /mnt/flume/data
        // properties.put("sinks", "sink1 sink2");
        properties.put("sinks", "sink1");
        properties.put("sink1.type", "avro");
        // properties.put("sink1.type", "logger");
        // properties.put("sink2.type", "avro");
//        properties.put("sink1.hostname", "jaibigdata.com");
        properties.put("sink1.hostname", "localhost");
        properties.put("sink1.port", "41414");
        // properties.put("sink2.hostname", "localhost");
        // properties.put("sink2.port", "5565");
        // properties.put("processor.type", "load_balance");
        properties.put("processor.type", "default");
        // properties.put("sinks", "sink1");
        // properties.put("sink1.type", "logger");
        // properties.put("sink1.channel", "channel");
        try
        {
            agent = new EmbeddedAgent("myagent");
            agent.configure(properties);
            agent.start();
        }
        catch (final Exception ex)
        {
            LOG.error("Error creating agent!", ex);
        }
    }
	@Override
	public EmbeddedAgent getFlumeAgent() {
		if (agent == null)
        {
            createAgent();
        }
        return agent;
	}

}
