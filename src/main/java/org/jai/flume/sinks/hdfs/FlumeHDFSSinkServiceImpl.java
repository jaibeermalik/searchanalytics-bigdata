package org.jai.flume.sinks.hdfs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.sink.hdfs.HDFSEventSink;
import org.jai.hadoop.HadoopClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class FlumeHDFSSinkServiceImpl implements FlumeHDFSSinkService {

	private static final Logger LOG = LoggerFactory
			.getLogger(FlumeHDFSSinkServiceImpl.class);

	private HDFSEventSink sink;
	private Channel channel;
	@Autowired
	private HadoopClusterService hadoopClusterService;

	@Override
	public Sink getSink() {
		return sink;
	}

	@Override
	public void processEvents(List<Event> events) {
		int batchSize = 10;
		int batches = events.size() / batchSize;
		for (int i = 0; i <= batches; i++) {
			Transaction txn = channel.getTransaction();
			txn.begin();
			int from = batchSize * i;
			int to = (i == batches) ? events.size() : from + batchSize;
			for (Event event : events.subList(from, to)) {
				channel.put(event);
				LOG.debug("Putting event to channel: {}", event);
			}
			txn.commit();
			txn.close();
			try {
				sink.process();
			} catch (EventDeliveryException e) {
				LOG.error("Error processing events!", e);
				throw new RuntimeException("Error processing events!", e);
			}
		}
	}

	@Override
	public void start() {
		createSink();
	}

	@Override
	public void shutdown() {
		channel.stop();
		sink.stop();
	}

	@Override
	public Channel getChannel() {
		return channel;
	}

	private void createSink() {
		sink = new HDFSEventSink();
		sink.setName("HDFSEventSink-" + UUID.randomUUID());
		channel = new MemoryChannel();
		Map<String, String> channelParamters = new HashMap<>();
		channelParamters.put("capacity", "1000");
		channelParamters.put("transactionCapacity", "1000");
		Context channelContext = new Context(channelParamters);
		Configurables.configure(channel, channelContext);
		channel.setName("HDFSEventSinkChannel-" + UUID.randomUUID());

		Map<String, String> paramters = new HashMap<>();
		paramters.put("hdfs.type", "hdfs");
		String hdfsBasePath = hadoopClusterService.getHDFSUri()
				+ "/searchevents";
		paramters.put("hdfs.path", hdfsBasePath + "/%Y/%m/%d/%H");
		paramters.put("hdfs.filePrefix", "searchevents");
		paramters.put("hdfs.fileType", "DataStream");
		paramters.put("hdfs.rollInterval", "0");
		paramters.put("hdfs.rollSize", "0");
		paramters.put("hdfs.idleTimeout", "1");
		paramters.put("hdfs.rollCount", "0");
		paramters.put("hdfs.batchSize", "1000");
		paramters.put("hdfs.useLocalTimeStamp", "true");

		Context sinkContext = new Context(paramters);
		sink.configure(sinkContext);
		sink.setChannel(channel);

		sink.start();
		channel.start();
	}

}
