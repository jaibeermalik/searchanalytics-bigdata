package org.jai.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.jai.search.analytics.GenerateSearchAnalyticsDataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.hadoop.pig.PigCallback;
import org.springframework.data.hadoop.pig.PigRunner;
import org.springframework.data.hadoop.pig.PigScript;
import org.springframework.data.hadoop.pig.PigTemplate;
import org.springframework.stereotype.Service;

@Service
public class PigScriptServiceImpl implements PigScriptService {

	private static final Logger LOG = LoggerFactory
			.getLogger(PigScriptServiceImpl.class);

	@Autowired
	private PigTemplate pigTemplate;

	@Autowired
	private PigRunner pigRunner;
	
	@Autowired
	private GenerateSearchAnalyticsDataService generateSearchAnalyticsDataService;

	@Override
	public void setup() {
	}

	@Override
	public long countTotalEvents() {
		try {
			final PigScript script = new PigScript(new ClassPathResource(
					"pig/count-all-search-events.pig"));
			return pigTemplate.execute(new PigCallback<Long>() {
				@Override
				public Long doInPig(PigServer pigServer) throws ExecException,
						IOException {
					pigServer.setBatchOn();
					pigServer.registerScript(script.getResource()
							.getInputStream());
					List<ExecJob> executeBatch = pigServer.executeBatch();
					long count = 0;
					for (ExecJob execJob : executeBatch) {
						count = (Long) execJob.getResults().next().get(0);
						LOG.debug("Pig Script Exec job result for total events: {}", count);
					}
					return count;
				}
			});
		} catch (Exception e) {
			String errMsg = "Error occurent while getting total count using pig script!";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
	}

	@Override
	public void getAllCustomerIds(String year, String month, String day, String hour) {
		try {

			String jsonSchema = generateSearchAnalyticsDataService.generateSearchQueryInstructionPIGJsonSchema();
			LOG.debug("Genrated json schema is: {}", jsonSchema);
			Collection<PigScript> scripts = new ArrayList<PigScript>();
			Map<String, String> args = new HashMap<>();
			args.put("JSONSCHEMA", jsonSchema);
			args.put("YEAR", year);
			args.put("MONTH", month);
			args.put("DAY", day);
			args.put("HOUR", hour);
			
			final PigScript script = new PigScript(new ClassPathResource(
					"pig/count-unique-customerids.pig"), args);
			scripts.add(script);
			
			pigRunner.setScripts(scripts);
			pigRunner.call();

		} catch (Exception e) {
			String errMsg = "Error occurent while getting unique customer ids using pig script!";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
	}

}
