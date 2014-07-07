package org.jai.flume.sinks.hbase;

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
import org.apache.flume.sink.hbase.HBaseSink;
import org.apache.flume.sink.hbase.HBaseSinkConfigurationConstants;
import org.jai.flume.sinks.hbase.serializer.HbaseJsonEventSerializer;
import org.jai.hadoop.HadoopClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class FlumeHbaseSinkServiceImpl implements FlumeHbaseSinkService{

	private static final Logger LOG = LoggerFactory
			.getLogger(FlumeHbaseSinkServiceImpl.class);

	private HBaseSink sink;
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
		
		channel = new MemoryChannel();
		Map<String, String> channelParamters = new HashMap<>();
		channelParamters.put("capacity", "100000");
		channelParamters.put("transactionCapacity", "1000");
		Context channelContext = new Context(channelParamters);
		Configurables.configure(channel, channelContext);
		channel.setName("HbaseSinkChannel-" + UUID.randomUUID());

		sink = new HBaseSink();
		sink.setName("HbaseSink-" + UUID.randomUUID());
		Map<String, String> paramters = new HashMap<>();
		paramters.put(HBaseSinkConfigurationConstants.CONFIG_TABLE, "searchclicks");
		paramters.put(HBaseSinkConfigurationConstants.CONFIG_COLUMN_FAMILY, new String(HbaseJsonEventSerializer.COLUMFAMILY_CLIENT_BYTES));
		paramters.put(HBaseSinkConfigurationConstants.CONFIG_BATCHSIZE, "1000");
//		paramters.put(HBaseSinkConfigurationConstants.CONFIG_SERIALIZER, RegexHbaseEventSerializer.class.getName());
//		paramters.put(HBaseSinkConfigurationConstants.CONFIG_SERIALIZER + "." + RegexHbaseEventSerializer.REGEX_CONFIG, RegexHbaseEventSerializer.REGEX_DEFAULT);
//		paramters.put(HBaseSinkConfigurationConstants.CONFIG_SERIALIZER + "." + RegexHbaseEventSerializer.IGNORE_CASE_CONFIG, "true");
//		paramters.put(HBaseSinkConfigurationConstants.CONFIG_SERIALIZER + "." + RegexHbaseEventSerializer.COL_NAME_CONFIG, "json");
		paramters.put(HBaseSinkConfigurationConstants.CONFIG_SERIALIZER, HbaseJsonEventSerializer.class.getName());

		
		Context sinkContext = new Context(paramters);
		sink.configure(sinkContext);
		sink.setChannel(channel);

		sink.start();
		channel.start();
	}

}
