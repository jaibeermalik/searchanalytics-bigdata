package org.jai.spark;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

@SuppressWarnings("serial")
public class QueryStringJDStreams implements Serializable {
	private static final Logger LOG = LoggerFactory
			.getLogger(QueryStringJDStreams.class);

	public JavaPairDStream<Integer, String> topQueryStringsCountInLastOneHour(
			JavaDStream<String> fileStream) {
		JavaDStream<String> onlyQueryStringStream = fileStream
				.filter(new Function<String, Boolean>() {

					@Override
					public Boolean call(String eventString) throws Exception {
						LOG.debug("Filtering the incoming event stream: {}",
								eventString);
						String queryString = getQueryString(eventString);
						if (queryString != null && queryString != ""
								&& queryString != "null") {
							LOG.debug("Valid querystring found : {}",
									queryString);
							return true;
						}
						return false;
					}

				});
		JavaPairDStream<String, Integer> queryStringStream = onlyQueryStringStream
				.map(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String eventString) {
						String queryString = getQueryString(eventString);
						return new Tuple2<String, Integer>(queryString, 1);
					}

				});

		JavaPairDStream<String, Integer> counts = queryStringStream
				.reduceByKeyAndWindow(
						new Function2<Integer, Integer, Integer>() {
							public Integer call(Integer i1, Integer i2) {
								return i1 + i2;
							}
						}, new Function2<Integer, Integer, Integer>() {
							public Integer call(Integer i1, Integer i2) {
								return i1 - i2;
							}
						}, new Duration(60 * 60 * 1000), new Duration(1 * 1000));

		JavaPairDStream<Integer, String> swappedCounts = counts
				.map(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					public Tuple2<Integer, String> call(
							Tuple2<String, Integer> in) {
						return in.swap();
					}
				});
		JavaPairDStream<Integer, String> sortedCounts = swappedCounts
				.transform(new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
					public JavaPairRDD<Integer, String> call(
							JavaPairRDD<Integer, String> in) throws Exception {
						return in.sortByKey(false);
					}
				});
//		sortedCounts
//				.foreach(new Function<JavaPairRDD<Integer, String>, Void>() {
//					public Void call(JavaPairRDD<Integer, String> rdd) {
//						LOG.debug("\nTop 10 query string:\n");
//						for (Tuple2<Integer, String> t : rdd.take(10)) {
//							LOG.debug(t.toString());
//						}
//						return null;
//					}
//				});
		return sortedCounts;
	}

	public JavaPairDStream<Integer, String> topProductViewsCountInLastOneHour(
			JavaDStream<String> fileStream) {
		JavaDStream<String> onlyQueryStringStream = fileStream
				.filter(new Function<String, Boolean>() {

					@Override
					public Boolean call(String eventString) throws Exception {
						LOG.debug("Filtering the incoming event stream: {}",
								eventString);
						String productIdString = getProductIdString(eventString);
						if (productIdString != null && productIdString != ""
								&& productIdString != "null") {
							LOG.debug("Valid productid found : {}",
									productIdString);
							return true;
						}
						return false;
					}

				});
		JavaPairDStream<String, Integer> productIdCountsStream = onlyQueryStringStream
				.map(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String eventString) {
						String productIdString = getProductIdString(eventString);
						return new Tuple2<String, Integer>(productIdString, 1);
					}

				});

		JavaPairDStream<String, Integer> counts = productIdCountsStream
				.reduceByKeyAndWindow(
						new Function2<Integer, Integer, Integer>() {
							public Integer call(Integer i1, Integer i2) {
								return i1 + i2;
							}
						}, new Function2<Integer, Integer, Integer>() {
							public Integer call(Integer i1, Integer i2) {
								return i1 - i2;
							}
						}, new Duration(60 * 60 * 1000), new Duration(1 * 1000));

		JavaPairDStream<Integer, String> swappedCounts = counts
				.map(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					public Tuple2<Integer, String> call(
							Tuple2<String, Integer> in) {
						return in.swap();
					}
				});
		JavaPairDStream<Integer, String> sortedCounts = swappedCounts
				.transform(new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
					public JavaPairRDD<Integer, String> call(
							JavaPairRDD<Integer, String> in) throws Exception {
						return in.sortByKey(false);
					}
				});
//		sortedCounts
//				.foreach(new Function<JavaPairRDD<Integer, String>, Void>() {
//					public Void call(JavaPairRDD<Integer, String> rdd) {
//						LOG.debug("\nTop 10 viewed product id:\n");
//						for (Tuple2<Integer, String> t : rdd.take(10)) {
//							LOG.debug(t.toString());
//						}
//						return null;
//					}
//				});
		return sortedCounts;
	}

	// For unit testing
	protected String getQueryString(String eventString) {
		String queryPattern = "\"querystring\":\"(\\w+)\"";
		Pattern pattern = Pattern.compile(queryPattern);
		Matcher matcher = pattern.matcher(eventString);
		String firstFind = null;
		if (matcher.find()) {
			firstFind = matcher.group(1);
			LOG.debug("\n querystring Found: " + firstFind);
		}
		return firstFind;
	}

	// For unit testing
	protected String getProductIdString(String eventString) {
		String queryPattern = "\"clickeddocid\":\"(\\w+)\"";
		Pattern pattern = Pattern.compile(queryPattern);
		Matcher matcher = pattern.matcher(eventString);
		String firstFind = null;
		if (matcher.find()) {
			firstFind = matcher.group(1);
			LOG.debug("\n ProductId Found: " + firstFind);
		}
		return firstFind;
	}

}
