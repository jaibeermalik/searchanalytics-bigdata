package org.jai.pig;

public interface PigScriptService {

	long countTotalEvents();

	void setup();

	void getAllCustomerIds(String year, String month, String day, String hour);
}
