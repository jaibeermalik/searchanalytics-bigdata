package org.jai.flume.agent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.agent.embedded.EmbeddedAgent;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.MultiplexingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.sink.RollingFileSink;
import org.apache.flume.source.AvroSource;
import org.jai.flume.sinks.elasticsearch.FlumeESSinkService;
import org.jai.flume.sinks.hdfs.FlumeHDFSSinkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class FlumeAgentServiceImpl implements FlumeAgentService {

	private static final Logger LOG = LoggerFactory
			.getLogger(FlumeAgentServiceImpl.class);

	@Autowired
	private FlumeHDFSSinkService flumeHDFSSinkService;
	@Autowired
	private FlumeESSinkService flumeESSinkService;

	private static EmbeddedAgent agent;
	// Ideally external avro source to consume embedded agent data. For testing
	// here.
	private AvroSource avroSource;
	private RollingFileSink sink;
	private Channel channel;

	@Override
	public void setup() {
		createAvroSourceWithSelectorHDFSAndESSinks();
		createAgent();
	}

	private void createAvroSourceWithSelectorHDFSAndESSinks() {
		Channel ESChannel = flumeESSinkService.getChannel();
		Channel HDFSChannel = flumeHDFSSinkService.getChannel();

		final Map<String, String> properties = new HashMap<String, String>();
		properties.put("type", "avro");
		properties.put("bind", "localhost");
		properties.put("port", "44444");

		avroSource = new AvroSource();
		avroSource.setName("AvroSource-" + UUID.randomUUID());
		Context sourceContext = new Context(properties);
		avroSource.configure(sourceContext);
		ChannelSelector selector = new MultiplexingChannelSelector();
		List<Channel> channels = new ArrayList<>();
		channels.add(ESChannel);
		channels.add(HDFSChannel);
		selector.setChannels(channels);
		final Map<String, String> selectorProperties = new HashMap<String, String>();
		selectorProperties.put("type", "multiplexing");
		selectorProperties.put("header", "State");
		selectorProperties.put("mapping.VIEWED", HDFSChannel.getName() + " "
				+ ESChannel.getName());
		selectorProperties.put("mapping.FAVOURITE", HDFSChannel.getName() + " "
				+ ESChannel.getName());
		selectorProperties.put("default", HDFSChannel.getName());
		Context selectorContext = new Context(selectorProperties);
		selector.configure(selectorContext);
		ChannelProcessor cp = new ChannelProcessor(selector);
		avroSource.setChannelProcessor(cp);

		avroSource.start();
	}

	// Note: Use in case just want to dump data to rolling file sink.
	@SuppressWarnings("unused")
	private void createAvroSourceWithLocalFileRollingSink() {
		channel = new MemoryChannel();
		String channelName = "AvroSourceMemoryChannel-" + UUID.randomUUID();
		channel.setName(channelName);

		sink = new RollingFileSink();
		sink.setName("RollingFileSink-" + UUID.randomUUID());
		Map<String, String> paramters = new HashMap<>();
		paramters.put("type", "file_roll");
		paramters.put("sink.directory", "target/flumefilelog");
		Context sinkContext = new Context(paramters);
		sink.configure(sinkContext);
		Configurables.configure(channel, sinkContext);
		sink.setChannel(channel);

		final Map<String, String> properties = new HashMap<String, String>();
		properties.put("type", "avro");
		properties.put("bind", "localhost");
		properties.put("port", "44444");
		properties.put("selector.type", "multiplexing");
		properties.put("selector.header", "State");
		properties.put("selector.mapping.VIEWED", channelName);
		properties.put("selector.mapping.default", channelName);

		avroSource = new AvroSource();
		avroSource.setName("AvroSource-" + UUID.randomUUID());
		Context sourceContext = new Context(properties);
		avroSource.configure(sourceContext);
		ChannelSelector selector = new MultiplexingChannelSelector();
		List<Channel> channels = new ArrayList<>();
		channels.add(channel);
		selector.setChannels(channels);
		final Map<String, String> selectorProperties = new HashMap<String, String>();
		properties.put("default", channelName);
		Context selectorContext = new Context(selectorProperties);
		selector.configure(selectorContext);
		ChannelProcessor cp = new ChannelProcessor(selector);
		avroSource.setChannelProcessor(cp);

		sink.start();
		channel.start();
		avroSource.start();
	}

	@Override
	public void shutdown() {
		if (agent != null) {
			agent.stop();
		}
		if (avroSource != null) {
			channel.stop();
			sink.stop();
			avroSource.stop();
		}
	}

	@Override
	public void processAllEvents() {
		try {
			// sleep 10 sec to be able to deliver events from embedded agent to
			// source.
			Thread.sleep(10000);
			// for (Channel channel :
			// avroSource.getChannelProcessor().getSelector().getAllChannels())
			// {
			// Transaction transaction = channel.getTransaction();
			// transaction.commit();
			// }
			flumeHDFSSinkService.getSink().process();
			flumeESSinkService.getSink().process();
		} catch (EventDeliveryException | InterruptedException e) {
			String errMsg = "Error processing event!";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
	}

	private void createAgent() {
		final Map<String, String> properties = new HashMap<String, String>();
		properties.put("channel.type", "memory");
		properties.put("channel.capacity", "100000");
		properties.put("channel.transactionCapacity", "1000");
		// a1.channels.c1.type = file
		// a1.channels.c1.checkpointDir = /mnt/flume/checkpoint
		// a1.channels.c1.dataDirs = /mnt/flume/data
		// properties.put("sinks", "sink1 sink2");
		properties.put("sinks", "sink1");
		properties.put("sink1.type", "avro");
		// properties.put("sink1.type", "logger");
		// properties.put("sink2.type", "avro");
		// properties.put("sink1.hostname", "jaibigdata.com");
		properties.put("sink1.hostname", "localhost");
		// properties.put("sink1.port", "41414");
		properties.put("sink1.port", "44444");
		// properties.put("sink2.hostname", "localhost");
		// properties.put("sink2.port", "5565");
		// properties.put("processor.type", "load_balance");
		properties.put("processor.type", "default");
		// properties.put("sinks", "sink1");
		// properties.put("sink1.type", "logger");
		// properties.put("sink1.channel", "channel");
		try {
			agent = new EmbeddedAgent("myagent");
			agent.configure(properties);
			agent.start();
		} catch (final Exception ex) {
			LOG.error("Error creating agent!", ex);
		}
	}

	@Override
	public EmbeddedAgent getFlumeAgent() {
		if (agent == null) {
			createAgent();
		}
		return agent;
	}

}
