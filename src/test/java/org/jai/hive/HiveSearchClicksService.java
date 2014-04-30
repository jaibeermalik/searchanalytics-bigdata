package org.jai.hive;

import java.util.List;

public interface HiveSearchClicksService {

	List<String> getDbs();
	
	List<String> getTables(String dnName);
	
	void setup();
}
