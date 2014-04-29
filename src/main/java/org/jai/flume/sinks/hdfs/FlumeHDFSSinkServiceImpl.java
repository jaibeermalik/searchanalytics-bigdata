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
import org.jai.hadoop.hdfs.HadoopClusterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class FlumeHDFSSinkServiceImpl implements FlumeHDFSSinkService {

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
		// TODO: do this in batches.
		int batchSize = 10;
		int batches = events.size() / batchSize;
		for (int i = 0; i <= batches; i++) {
			Transaction txn = channel.getTransaction();
			txn.begin();
			int from = batchSize * i;
			int to = (i == batches) ? events.size() : from + batchSize;
			for (Event event : events.subList(from, to)) {
				channel.put(event);
//				System.out.println("Putting event to channel: " + event);
			}
			txn.commit();
			txn.close();
			try {
				sink.process();
			} catch (EventDeliveryException e) {
				e.printStackTrace();
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

	private void createSink() {
		sink = new HDFSEventSink();
		sink.setName("HDFSEventSink-" + UUID.randomUUID());
		channel = new MemoryChannel();
		channel.setName("HDFSEventSinkChannel-" + UUID.randomUUID());

		Map<String, String> paramters = new HashMap<>();
		paramters.put("hdfs.type", "hdfs");
		String hdfsBasePath = hadoopClusterService.getHDFSUri()
				+ "/searchevents";
		paramters.put("hdfs.path", hdfsBasePath + "/%Y/%m/%d/%H");
		paramters.put("hdfs.filePrefix", "searchevents");
		// paramters.put("hdfs.inUsePrefix", "eventstemp");
		// paramters.put("hdfs.inUseSuffix", ".tmp");
		paramters.put("hdfs.fileType", "DataStream");
		// paramters.put("hdfs.writeFormat", "Text");
		paramters.put("hdfs.rollInterval", "0");
		// paramters.put("hdfs.rollSize", "134217728");
		paramters.put("hdfs.rollSize", "0");
		paramters.put("hdfs.idleTimeout", "1");
		paramters.put("hdfs.rollCount", "0");
		paramters.put("hdfs.batchSize", "10");
		paramters.put("hdfs.useLocalTimeStamp", "true");

		Context sinkContext = new Context(paramters);
		// sink.setName("HDFSEventSink-" + UUID.randomUUID().toString());
		sink.configure(sinkContext);
		Configurables.configure(channel, sinkContext);
		sink.setChannel(channel);

		sink.start();
		channel.start();
	}

}
