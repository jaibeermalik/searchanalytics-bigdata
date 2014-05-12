package org.jai.hive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.service.HiveClient;
import org.jai.hadoop.HadoopClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.hadoop.hive.HiveClientCallback;
import org.springframework.data.hadoop.hive.HiveRunner;
import org.springframework.data.hadoop.hive.HiveScript;
import org.springframework.data.hadoop.hive.HiveTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class HiveSearchClicksServiceImpl implements HiveSearchClicksService {

	private static final Logger LOG = LoggerFactory
			.getLogger(HiveSearchClicksServiceImpl.class);

	@Autowired
	private HiveTemplate hiveTemplate;
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private HadoopClusterService hadoopClusterService;

	@Autowired
	private HiveRunner hiveRunner;

	@Override
	public List<String> getDbs() {
		return hiveTemplate.execute(new HiveClientCallback<List<String>>() {
			@Override
			public List<String> doInHive(HiveClient hiveClient)
					throws Exception {
				return hiveClient.get_all_databases();
			}
		});
	}

	@Override
	public List<String> getTables(final String dnName) {
		return hiveTemplate.execute(new HiveClientCallback<List<String>>() {
			@Override
			public List<String> doInHive(HiveClient hiveClient)
					throws Exception {
				return hiveClient.get_all_tables(dnName);
			}
		});
	}

	private void setupSearchClicksTable() {
		try {
			Collection<HiveScript> scripts = new ArrayList<>();
			HiveScript script = new HiveScript(new ClassPathResource(
					"hive/create-searchevents-table.q"));
			scripts.add(script);
			hiveRunner.setScripts(scripts);
			hiveRunner.call();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(
					"Failed to setup search_clicks table in hive!", e);
		}
	}

	private void setupSearchDatabase() {
		try {
			Collection<HiveScript> scripts = new ArrayList<>();
			HiveScript script = new HiveScript(new ClassPathResource(
					"hive/drop-create-search-database.q"));
			scripts.add(script);
			hiveRunner.setScripts(scripts);
			hiveRunner.call();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(
					"Failed to setup search_clicks table in hive!", e);
		}
	}

	@Override
	public void addPartition(final String dbName, final String tbName,
			final String year, final String month, final String day,
			final String hour) {
		try {
			Collection<HiveScript> scripts = new ArrayList<>();
			Map<String, String> args = new HashMap<>();
			args.put("DBNAME", dbName);
			args.put("TBNAME", tbName);
			args.put("YEAR", year);
			args.put("MONTH", month);
			args.put("DAY", day);
			args.put("HOUR", hour);
			HiveScript script = new HiveScript(new ClassPathResource(
					"hive/add_partition_searchevents.q"), args);
			scripts.add(script);
			hiveRunner.setScripts(scripts);
			hiveRunner.call();
		} catch (Exception e) {
			String errMsg = "Failed to add partition search_clicks table in hive!";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
		hiveTemplate.execute(new HiveClientCallback<Integer>() {
			@Override
			public Integer doInHive(HiveClient hiveClient) throws Exception {
				List<Partition> get_partitions = hiveClient.get_partitions(
						dbName, tbName, Short.MAX_VALUE);
				for (Partition partition1 : get_partitions) {
					LOG.debug("Partition Info tableName: {}",
							partition1.getTableName());
					LOG.debug("Partition Info creationTime: {}",
							partition1.getCreateTime());
					LOG.debug("Partition Info Location: {}", partition1.getSd()
							.getLocation());
					LOG.debug("Partition Info Values: {}",
							partition1.getValues());
				}
				return 0;
			}
		});
	}

	@Override
	public void getSearchClicks(String dbName, String tbName, String year,
			String month, String day, String hour) {
		try {
			Collection<HiveScript> scripts = new ArrayList<>();
			Map<String, String> args = new HashMap<>();
			args.put("DBNAME", dbName);
			args.put("TBNAME", tbName);
			args.put("YEAR", year);
			args.put("MONTH", month);
			args.put("DAY", day);
			args.put("HOUR", hour);
			HiveScript script = new HiveScript(new ClassPathResource(
					"hive/get_searchevents.q"), args);
			scripts.add(script);
			hiveRunner.setScripts(scripts);
			List<String> call = hiveRunner.call();
			for (String string : call) {
				LOG.debug("val is: " + string);
			}
		} catch (Exception e) {
			String errMsg = "Failed to get search_clicks table data in hive!";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
	}

	@Override
	public void loadSearchCustomerQueryTable() {
		try {
			// Load last one month top queries per customer.
			Collection<HiveScript> scripts = new ArrayList<>();
			HiveScript script = new HiveScript(new ClassPathResource(
					"hive/load-search_customerquery-table.q"));
			scripts.add(script);
			hiveRunner.setScripts(scripts);
			hiveRunner.call();
//			hiveRunner.
		} catch (Exception e) {
			String errMsg = "Failed to loadSearchCustomerQueryTable in hive!";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
	}

	@Override
	public void loadTopSearchCustomerQueryToElasticSearchIndex() {
		try {
			// Load last one month top queries per customer.
			Collection<HiveScript> scripts = new ArrayList<>();
			HiveScript script = new HiveScript(new ClassPathResource(
					"hive/load-search_customerquery_to_es.q"));
			scripts.add(script);
			hiveRunner.setScripts(scripts);
			hiveRunner.call();
		} catch (Exception e) {
			String errMsg = "Failed to loadTopSearchCustomerQueryToElasticSearchIndex in hive!";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
	}

	@Override
	public int getTotalRowCount(String dbName, String tbName) {
		String sql = "select count(0) from " + dbName + "." + tbName;
		Integer queryForObject = jdbcTemplate
				.queryForObject(sql, Integer.class);
		LOG.debug("JDBC template count: {}", queryForObject);

		return queryForObject;
		// return hiveTemplate.execute(new HiveClientCallback<Integer>() {
		// @Override
		// public Integer doInHive(HiveClient hiveClient) throws Exception {
		// return hiveClient.fetchAll().size();
		// }
		// });
	}

	@Override
	public void setup() {
		setupSearchDatabase();
		setupSearchClicksTable();
	}

}
