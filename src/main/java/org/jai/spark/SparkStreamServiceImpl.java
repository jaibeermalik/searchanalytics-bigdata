package org.jai.spark;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import org.jai.hadoop.HadoopClusterService;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SparkStreamServiceImpl implements SparkStreamService {

	private static final Logger LOG = LoggerFactory
			.getLogger(SparkStreamServiceImpl.class);

	@Autowired
	private HadoopClusterService hadoopClusterService;
	private JavaStreamingContext jssc;

	@Override
	public void setup() {
		// Create a StreamingContext with a SparkConf configuration
		SparkConf sparkConf = new SparkConf(false)
				.setAppName("JaiSpark")
				.setSparkHome("target/sparkhome")
				.setMaster("local")
				.set("spark.executor.memory", "128m")
				.set("spark.local.dir",
						new File("target/sparkhome/tmp").getAbsolutePath())
				.set("spark.cores.max", "2").set("spark.akka.threads", "2")
				.set("spark.akka.timeout", "60").set("spark.logConf", "true")
				.set("spark.cleaner.delay", "3700")
				.set("spark.cleaner.ttl", "86400")
				.set("spark.shuffle.spill", "false")
				.set("spark.driver.host", "localhost")
				.set("spark.driver.port", "43214");
		jssc = new JavaStreamingContext(sparkConf, new Duration(5000));

		String checkpointDir = hadoopClusterService.getHDFSUri()
				+ "/sparkcheckpoint";
		jssc.checkpoint(checkpointDir);
		startFlumeStream();
	}

	@Override
	public void shutdown() {
		jssc.stop();
	}

	@Override
	public void startHDFSTxtFileStreams() {
		String hdfsUri = hadoopClusterService.getHDFSUri() + "/searchevents"
				+ getCurrentStreamUri();

		QueryStringJDStreams queryStringJDStreams = new QueryStringJDStreams();

		JavaDStream<String> fileStream = jssc.textFileStream(hdfsUri);
		queryStringJDStreams.topQueryStringsCountInLastOneHour(fileStream);

		queryStringJDStreams.topProductViewsCountInLastOneHour(fileStream);

		LOG.debug("Starting streaming context!");
		jssc.start();
		LOG.debug("Streaming context running!");
	}

	@Override
	public void startFlumeStream() {
		JavaDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createStream(
				jssc, "localhost", 41111, StorageLevels.MEMORY_AND_DISK);

		QueryStringJDStreams queryStringJDStreams = new QueryStringJDStreams();

		// Run top top search query string stream
		queryStringJDStreams
				.topQueryStringsCountInLastOneHourUsingSparkFlumeEvent(flumeStream);

		// Run top product view stream
		//TODO: uncomment to get both stats.
//		queryStringJDStreams
//				.topProductViewsCountInLastOneHourUsingSparkFlumeEvent(flumeStream);
		jssc.start();
	}

	@Override
	public void storeCustomerFavourites() {

		// fileStream.saveAsHadoopFiles("spark", "data");
		// JavaPairDStream<Object, Object> filter = fileStream
		// .filter(new Function<Tuple2<Object,Object>, Boolean>() {
		//
		// @Override
		// public Boolean call(Tuple2<Object, Object> arg0) throws Exception {
		// System.out.println(arg0);
		// return true;
		// }
		// });
		// filter.print();

		// jssc.wstop
		// SparkFlumeEvent event = new SparkFlumeEvent();
		// event.
		// JavaDStream<SparkFlumeEvent> flumeStream =
		// FlumeUtils.createStream(jssc,
		// "localhost", 42424);
		//
		// //only return favourite ones.
		// JavaDStream<SparkFlumeEvent> filter = flumeStream.filter(new
		// Function<SparkFlumeEvent, Boolean>(){
		// @Override
		// public Boolean call(SparkFlumeEvent sparkEvent) throws Exception {
		// System.out.println(sparkEvent);
		// return sparkEvent.event().getHeaders().containsKey("FAVOURITE");
		// }
		// });
		//
		// //store all favourite in data with customerid, productid
		//

	}

	private String getCurrentStreamUri() {
		// Add partition
		DateTime now = new DateTime();
		int monthOfYear = now.getMonthOfYear();
		int dayOfMonth = now.getDayOfMonth();
		int hourOfDay = now.getHourOfDay();
		String year = String.valueOf(now.getYear());
		String month = monthOfYear < 10 ? "0" + String.valueOf(monthOfYear)
				: String.valueOf(monthOfYear);
		String day = dayOfMonth < 10 ? "0" + String.valueOf(dayOfMonth)
				: String.valueOf(dayOfMonth);
		String hour = hourOfDay < 10 ? "0" + String.valueOf(hourOfDay) : String
				.valueOf(hourOfDay);
		return "/" + year + "/" + month + "/" + day + "/" + hour;
	}

}
