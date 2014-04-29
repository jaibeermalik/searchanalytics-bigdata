package org.jai.hive;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.hadoop.hive.service.HiveClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hive.HiveClientCallback;
import org.springframework.data.hadoop.hive.HiveRunner;
import org.springframework.data.hadoop.hive.HiveTemplate;
import org.springframework.stereotype.Service;

@Service
public class HiveSearchClicksServiceImpl implements HiveSearchClicksService {

	@Autowired
	private HiveTemplate hiveTemplate;

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

	public void setupSearchClicksTable() {
		Collection<Callable<?>> actions = null;
		hiveRunner.setPreAction(actions);
		try {
			hiveRunner.call();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(
					"Failed to setup search_clicks table in hive!", e);
		}
	}

}
