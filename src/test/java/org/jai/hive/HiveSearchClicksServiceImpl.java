package org.jai.hive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.service.HiveClient;
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

	@Autowired
	private HiveTemplate hiveTemplate;
	@Autowired
	private JdbcTemplate jdbcTemplate;
	

	@Autowired
	private HiveRunner hiveRunner;

	// private JdbcTemplate template;

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

	private void setupSearchClicksExternalTable() {
		try {
			Collection<HiveScript> scripts = new ArrayList<>();
			HiveScript script = new HiveScript(new ClassPathResource("hive/create-searchevents-externaltable.q"));
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
			HiveScript script = new HiveScript(new ClassPathResource("hive/drop-create-search-database.q"));
			scripts.add(script);
			hiveRunner.setScripts(scripts);
			hiveRunner.call();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(
					"Failed to setup search_clicks table in hive!", e);
		}
//		hiveTemplate.execute(new HiveClientCallback<Database>() {
//			@Override
//			public Database doInHive(HiveClient hiveClient)
//					throws Exception {
//				String dbName = "search";
//				hiveClient.drop_database(dbName, true, true);
//				Database database = new Database();
//				database.setName(dbName);
//				hiveClient.create_database(database);
//				return database;
//			}
//		});
	}
	
	@Override
	public int getTotalSearchClicksCount() {
//		Integer queryForObject = jdbcTemplate.queryForObject("select count(*) from search.search_clicks", Integer.class);
//		return queryForObject;
		hiveTemplate.execute(new HiveClientCallback<Integer>() {
			@Override
			public Integer doInHive(HiveClient hiveClient)
					throws Exception {
				List<Partition> get_partitions = hiveClient.get_partitions("search", "search_clicks", Short.MAX_VALUE);
				for (Partition partition : get_partitions) {
					System.out.println("par: " + partition.getCreateTime());
				}
				return 0;
			}
		});
		return hiveTemplate.execute(new HiveClientCallback<Integer>() {
			@Override
			public Integer doInHive(HiveClient hiveClient)
					throws Exception {
				return hiveClient.fetchAll().size();
			}
		});
	}
	
	@Override
	public void setup() {
		setupSearchDatabase();
		setupSearchClicksExternalTable();
	}

}
