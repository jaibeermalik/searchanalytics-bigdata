package org.jai.spark;

public interface SparkStreamService {

	void storeCustomerFavourites();

	void setup();

	void shutdown();

	void startHDFSTxtFileStreams();

	void startFlumeStream();

}
