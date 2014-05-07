package org.jai.flume.sinks.elasticsearch;

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
import org.apache.flume.sink.elasticsearch.ElasticSearchSink;
import org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants;
import org.jai.flume.sinks.elasticsearch.serializer.ElasticSearchJsonBodyEventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class FlumeESSinkServiceImpl implements FlumeESSinkService {

	private static final Logger LOG = LoggerFactory
			.getLogger(FlumeESSinkServiceImpl.class);

	private ElasticSearchSink sink;
	private Channel channel;

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
				LOG.debug("Putting event to channel: {} ", event);
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
		sink = new ElasticSearchSink();
		sink.setName("ElasticSearchSink-" + UUID.randomUUID());
		channel = new MemoryChannel();
		Map<String, String> channelParamters = new HashMap<>();
		channelParamters.put("capacity", "1000");
		channelParamters.put("transactionCapacity", "1000");
		Context channelContext = new Context(channelParamters);
		Configurables.configure(channel, channelContext);
		channel.setName("ElasticSearchSinkChannel-" + UUID.randomUUID());

		Map<String, String> paramters = new HashMap<>();
		paramters.put(ElasticSearchSinkConstants.HOSTNAMES, "127.0.0.1:9310");
		String indexNamePrefix = "recentlyviewed";
		paramters.put(ElasticSearchSinkConstants.INDEX_NAME, indexNamePrefix);
		paramters.put(ElasticSearchSinkConstants.INDEX_TYPE, "clickevent");
		paramters.put(ElasticSearchSinkConstants.CLUSTER_NAME,
				"jai-testclusterName");
		paramters.put(ElasticSearchSinkConstants.BATCH_SIZE, "10");
		paramters.put(ElasticSearchSinkConstants.SERIALIZER,
				ElasticSearchJsonBodyEventSerializer.class.getName());

		Context sinkContext = new Context(paramters);
		sink.configure(sinkContext);
		sink.setChannel(channel);

		sink.start();
		channel.start();
	}

}
