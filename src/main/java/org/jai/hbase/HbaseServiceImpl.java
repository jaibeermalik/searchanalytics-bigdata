package org.jai.hbase;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.jai.flume.sinks.hbase.serializer.HbaseJsonEventSerializer;
import org.jai.hadoop.HadoopClusterService;
import org.jai.search.model.SearchFacetName;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.stereotype.Service;

import com.google.common.base.Functions;
import com.google.common.collect.Ordering;

@Service
public class HbaseServiceImpl implements HbaseService {

	private static final Logger LOG = LoggerFactory
			.getLogger(HbaseServiceImpl.class);
	@Autowired
	private HbaseTemplate hbaseTemplate;
	@Autowired
	private HadoopClusterService hadoopClusterService;

	private MiniHBaseCluster miniHBaseCluster;
	private MiniZooKeeperCluster miniZooKeeperCluster;

	@Override
	public void setup() {
		try {
			LOG.info("Setting up Hbase mini cluster!");
			File clusterTestDirRoot = new File("target/zookeeper");
			clusterTestDirRoot.delete();
			File clusterTestDir = new File(clusterTestDirRoot, "/dfscluster_"
					+ UUID.randomUUID().toString()).getAbsoluteFile();

			LOG.info("Setting up Hbase zookeeper mini cluster!");
			int clientPort = 10235;
			miniZooKeeperCluster = new MiniZooKeeperCluster();
			miniZooKeeperCluster.setDefaultClientPort(clientPort);
			miniZooKeeperCluster.startup(clusterTestDir);
			LOG.info("Setting up Hbase zookeeper mini cluster done!");

			LOG.info("Setting up Hbase mini cluster master!");
			Configuration config = HBaseConfiguration.create();
			config.set("hbase.tmp.dir",
					new File("target/hbasetom").getAbsolutePath());
			config.set("hbase.rootdir", hadoopClusterService.getHDFSUri()
					+ "/hbase");
			config.set("hbase.master.port", "44335");
			config.set("hbase.master.info.port", "44345");
			config.set("hbase.regionserver.port", "44435");
			config.set("hbase.regionserver.info.port", "44445");
			config.set("hbase.master.distributed.log.replay", "false");
			config.set("hbase.cluster.distributed", "false");
			config.set("hbase.master.distributed.log.splitting", "false");
			// hbase.zookeeper.peerport
			// hbase.zookeeper.leaderport
			config.set("hbase.zookeeper.property.clientPort",
					Integer.toString(clientPort));
			config.set("zookeeper.znode.parent", "/hbase");
			
			miniHBaseCluster = new MiniHBaseCluster(config, 1);
			miniHBaseCluster.startMaster();

			LOG.info("Setting up Hbase mini cluster done!");
		} catch (IOException | InterruptedException e) {
			String errMsg = "Error occured starting Mini Hbase cluster";
			LOG.error(errMsg);
			throw new RuntimeException(errMsg, e);
		}

		setupSearchEventsTable();
	}

	@Override
	public void shutdown() {
		try {
			miniHBaseCluster.stopRegionServer(1);
			miniHBaseCluster.stopMaster(1);
			miniHBaseCluster.waitUntilShutDown(); //shutdown();
			miniZooKeeperCluster.shutdown();
		} catch (IOException e) {
			// Don't do anything.
			// e.printStackTrace();
		}
	}

	@Override
	public void setupSearchEventsTable() {
		LOG.debug("Setting up searchclicks table!");
		String tableName = "searchclicks";
		TableName name = TableName.valueOf(tableName);
		HTableDescriptor desc = new HTableDescriptor(name);
		desc.addFamily(new HColumnDescriptor(
				HbaseJsonEventSerializer.COLUMFAMILY_CLIENT_BYTES));
		desc.addFamily(new HColumnDescriptor(
				HbaseJsonEventSerializer.COLUMFAMILY_CLIENT_BYTES));
		desc.addFamily(new HColumnDescriptor(
				HbaseJsonEventSerializer.COLUMFAMILY_SEARCH_BYTES));
		desc.addFamily(new HColumnDescriptor(
				HbaseJsonEventSerializer.COLUMFAMILY_FILTERS_BYTES));
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(miniHBaseCluster.getConf());
			hBaseAdmin.createTable(desc);
			hBaseAdmin.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		LOG.debug("Setting up searchclicks table done!");
	}

	@Override
	public void insertEventData(final byte[] body) {
		LOG.debug("Inserting searchclicks table row content event!");
		hbaseTemplate.execute("searchclicks", new TableCallback<Object>() {

			@Override
			public Object doInTable(HTableInterface table) throws Throwable {
				String rowId = UUID.randomUUID().toString();
				Put p = new Put(Bytes.toBytes(rowId));
				LOG.debug("Inserting searchclicks table row id: {}", rowId);
				p.add(HbaseJsonEventSerializer.COLUMFAMILY_CLIENT_BYTES, Bytes.toBytes("eventid"), body);
				table.put(p);
				table.close();
				return null;
			}
		});
		LOG.debug("Inserting searchclicks table row content event done!");
	}

	@Override
	public void getSearchClicks() {
		LOG.debug("Checking searchclicks table content!");
		Scan scan = new Scan();
		scan.addFamily(HbaseJsonEventSerializer.COLUMFAMILY_CLIENT_BYTES);
		scan.addFamily(HbaseJsonEventSerializer.COLUMFAMILY_SEARCH_BYTES);
		scan.addFamily(HbaseJsonEventSerializer.COLUMFAMILY_FILTERS_BYTES);
		List<String> rows = hbaseTemplate.find("searchclicks", scan,
				new RowMapper<String>() {
					@Override
					public String mapRow(Result result, int rowNum)
							throws Exception {
						return Arrays.toString(result.rawCells());
					}
				});
		for (String row : rows) {
			LOG.debug("searchclicks table content, Table returned row: {}", row);
		}
		LOG.debug("Checking searchclicks table content done!");
	}

	@Override
	public int getTotalSearchClicksCount() {
		LOG.debug("Checking searchclicks table count!");
		int totalCount = 0;
		List<String> rows = hbaseTemplate.find("searchclicks", new String(
				HbaseJsonEventSerializer.COLUMFAMILY_CLIENT_BYTES),
				new RowMapper<String>() {
					@Override
					public String mapRow(Result result, int rowNum)
							throws Exception {
						// return new
						// String(result.getValue(Bytes.toBytes("event"),
						// Bytes.toBytes("json")));
						 return new String(result.value());
					}
				});
		for (String row : rows) {
			LOG.debug("Table count returned row is : {}", row);
			totalCount++;
		}
		LOG.debug("Checking searchclicks table count done!");
		return totalCount;
	}

	@Override
	public void removeAll() {
		LOG.debug("Setting up searchclicks table!");
		String tableName = "searchclicks";
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(miniHBaseCluster.getConf());
			hBaseAdmin.disableTable(tableName);
			hBaseAdmin.deleteTable(tableName);
			hBaseAdmin.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		;
		setupSearchEventsTable();
		LOG.debug("Setting up searchclicks table done!");
	}

	@Override
	public void testHbaseServer() {
		LOG.debug("Testing hbase server!");

		String tableName = "MyTable";
		TableName name = TableName.valueOf(tableName);
		HTableDescriptor desc = new HTableDescriptor(name);
		HColumnDescriptor columnFamily = new HColumnDescriptor(
				"SomeColumn".getBytes());
		desc.addFamily(columnFamily);
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(miniHBaseCluster.getConf());
			hBaseAdmin.createTable(desc);
			hBaseAdmin.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		;

		// writing to 'MyTable'
		hbaseTemplate.execute(tableName, new TableCallback<Object>() {

			@Override
			public Object doInTable(HTableInterface table) throws Throwable {
				Put p = new Put(Bytes.toBytes("SomeRow"));
				p.add(Bytes.toBytes("SomeColumn"),
						Bytes.toBytes("SomeQualifier"), Bytes.toBytes("AValue"));
				table.put(p);
				return null;
			}
		});

		// read each row from 'MyTable'
		List<String> rows = hbaseTemplate.find("MyTable", "SomeColumn",
				new RowMapper<String>() {
					@Override
					public String mapRow(Result result, int rowNum)
							throws Exception {
						return result.toString();
					}
				});
		for (String row : rows) {
			System.out.println("Printing row:" + row);
		}
		LOG.debug("Hbase server testing done!");
	}

	@Override
	public int findTotalRecordsForValidCustomers() {
		int totalCount = 0;
		List<String> rows = hbaseTemplate.find("searchclicks", new String(
				HbaseJsonEventSerializer.COLUMFAMILY_CLIENT_BYTES),
				new RowMapper<String>() {
					@Override
					public String mapRow(Result result, int rowNum)
							throws Exception {
						String customerid = new String(
								result.getValue(
										HbaseJsonEventSerializer.COLUMFAMILY_CLIENT_BYTES,
										Bytes.toBytes("customerid")));
						return customerid;
					}
				});
		for (String row : rows) {
			LOG.debug(
					"Table count findTotalRecordsForValidCustomers returned row is : {}",
					row);
			if (row != null) {
				totalCount++;
			}
		}
		LOG.debug(
				"findTotalRecordsForValidCustomers searchclicks table count is: {}",
				totalCount);
		return totalCount;
	}

	@Override
	public List<String> findTopTenSearchQueryStringForLastAnHour() {
		Scan scan = new Scan();
		scan.addColumn(HbaseJsonEventSerializer.COLUMFAMILY_CLIENT_BYTES,
				Bytes.toBytes("createdtimestampinmillis"));
		scan.addColumn(HbaseJsonEventSerializer.COLUMFAMILY_SEARCH_BYTES,
				Bytes.toBytes("querystring"));
		List<String> topQueries = new ArrayList<>();
		Map<String, Integer> counts = new HashMap<>();
		List<String> rows = hbaseTemplate.find("searchclicks", scan,
				new RowMapper<String>() {
					@Override
					public String mapRow(Result result, int rowNum)
							throws Exception {
						String createdtimestampinmillis = new String(
								result.getValue(
										HbaseJsonEventSerializer.COLUMFAMILY_CLIENT_BYTES,
										Bytes.toBytes("createdtimestampinmillis")));
						byte[] value = result
								.getValue(
										HbaseJsonEventSerializer.COLUMFAMILY_SEARCH_BYTES,
										Bytes.toBytes("querystring"));
						String querystring = null;
						if (value != null) {
							querystring = new String(value);
						}
						LOG.debug(
								"findTopTenSearchQueryStringForLastAnHour returned row is, time: {} , querystring: {}",
								new Object[] { createdtimestampinmillis,
										querystring });

						if (new DateTime(Long.valueOf(createdtimestampinmillis))
								.plusHours(1).compareTo(new DateTime()) == 1
								&& querystring != null) {
							return querystring;
						}
						return null;
					}
				});
		for (String row : rows) {
			if (row != null) {
				Integer integer = counts.get(row);
				if (integer == null) {
					counts.put(row, Integer.valueOf(1));
				} else {
					counts.put(row, Integer.valueOf(integer.intValue() + 1));
				}
				LOG.debug(
						"findTopTenSearchQueryStringForLastAnHour valid query string value is : {}",
						row);
			}
		}

		List<String> sortedKeys = Ordering.natural()
				.onResultOf(Functions.forMap(counts))
				.immutableSortedCopy(counts.keySet());
		int recordsCount = sortedKeys.size();
		for (int j = 1; j <= 10 && j < sortedKeys.size(); j++) {
			String queryString = sortedKeys.get(recordsCount - j);
			LOG.debug("Top queries are sortedKeys, query: {}",
					new Object[] { queryString });
			topQueries.add(queryString);
		}
		// Ordering<String> onResultOf = Ordering.natural().onResultOf(
		// Functions.forMap(counts)).compound(Ordering.natural());
		// for (Entry<String, Integer> querString : ImmutableSortedMap.copyOf(
		// counts, onResultOf).entrySet()) {
		// LOG.debug("Top queries are, query: {}, count: {}", new Object[] {
		// querString.getKey(), querString.getValue() });
		// topQueries.add(querString.getKey());
		// }
		LOG.debug("Checking findTopTenSearchQueryStringForLastAnHour done!");
		return topQueries;
	}

	@Override
	public List<String> findTopTenSearchFiltersForLastAnHour() {
		Scan scan = new Scan();
		scan.addColumn(HbaseJsonEventSerializer.COLUMFAMILY_CLIENT_BYTES,
				Bytes.toBytes("createdtimestampinmillis"));
		for (String facetField : SearchFacetName.categoryFacetFields) {
			scan.addColumn(HbaseJsonEventSerializer.COLUMFAMILY_FILTERS_BYTES,
					Bytes.toBytes(facetField));
		}
		List<String> topFacetFilters = new ArrayList<>();
		final Map<String, List<String>> columnData = new HashMap<>();
		hbaseTemplate.find("searchclicks", scan, new RowMapper<String>() {
			@Override
			public String mapRow(Result result, int rowNum) throws Exception {
				String createdtimestampinmillis = new String(result.getValue(
						HbaseJsonEventSerializer.COLUMFAMILY_CLIENT_BYTES,
						Bytes.toBytes("createdtimestampinmillis")));
				for (String facetField : SearchFacetName.categoryFacetFields) {
					byte[] value = result.getValue(
							HbaseJsonEventSerializer.COLUMFAMILY_FILTERS_BYTES,
							Bytes.toBytes(facetField));
					if (value != null
							&& new DateTime(Long
									.valueOf(createdtimestampinmillis))
									.plusHours(1).compareTo(new DateTime()) == 1) {
						String facetValue = new String(value);
						LOG.debug("Facet field: {} and Facet Value: {}",
								new Object[] { facetField, facetValue });
						List<String> list = columnData.get(facetField);
						if (list == null) {
							list = new ArrayList<>();
							list.add(facetValue);
							columnData.put(facetField, list);
						} else {
							list.add(facetValue);
						}
					}
				}
				return null;
			}
		});

		final Map<String, Integer> counts = new HashMap<>();
		String separatorToken = "_jaijai_";
		for (Entry<String, List<String>> entry : columnData.entrySet()) {
			for (String facetFilterValue : entry.getValue()) {
				String key = entry.getKey() + separatorToken + facetFilterValue;
				Integer integer = counts.get(key);
				if (integer == null) {
					counts.put(key, Integer.valueOf(1));
				} else {
					counts.put(key, Integer.valueOf(integer.intValue() + 1));
				}
			}
		}

		List<String> sortedKeys = Ordering.natural()
				.onResultOf(Functions.forMap(counts))
				.immutableSortedCopy(counts.keySet());
		for (int j = 1; j <= 10 && j < sortedKeys.size(); j++) {
			String queryString = sortedKeys.get(sortedKeys.size() - j);
			String[] split = queryString.split(separatorToken);
			LOG.debug(
					"Top 10 filters are sortedKeys, FacetCode: {}, FacetValue:{}, Count:{}",
					new Object[] { split[0], split[1], counts.get(queryString) });
			topFacetFilters.add(split[1]);
		}
		LOG.debug("Checking findTopTenSearchQueryStringForLastAnHour done!");
		return topFacetFilters;
	}
	
	@Override
	public List<String> findTopTenSearchFiltersForLastAnHourUsingRangeScan() {
		Scan scan = new Scan();
		for (String facetField : SearchFacetName.categoryFacetFields) {
			scan.addColumn(HbaseJsonEventSerializer.COLUMFAMILY_FILTERS_BYTES,
					Bytes.toBytes(facetField));
		}
		DateTime dateTime = new DateTime();
		try {
			scan.setTimeRange(dateTime.minusHours(1).getMillis(), dateTime.getMillis());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		List<String> topFacetFilters = new ArrayList<>();
		final Map<String, List<String>> columnData = new HashMap<>();
		hbaseTemplate.find("searchclicks", scan, new RowMapper<String>() {
			@Override
			public String mapRow(Result result, int rowNum) throws Exception {
				for (String facetField : SearchFacetName.categoryFacetFields) {
					byte[] value = result.getValue(
							HbaseJsonEventSerializer.COLUMFAMILY_FILTERS_BYTES,
							Bytes.toBytes(facetField));
					if (value != null) {
						String facetValue = new String(value);
						LOG.debug("Facet field: {} and Facet Value: {}",
								new Object[] { facetField, facetValue });
						List<String> list = columnData.get(facetField);
						if (list == null) {
							list = new ArrayList<>();
							list.add(facetValue);
							columnData.put(facetField, list);
						} else {
							list.add(facetValue);
						}
					}
				}
				return null;
			}
		});

		final Map<String, Integer> counts = new HashMap<>();
		String separatorToken = "_jaijai_";
		for (Entry<String, List<String>> entry : columnData.entrySet()) {
			for (String facetFilterValue : entry.getValue()) {
				String key = entry.getKey() + separatorToken + facetFilterValue;
				Integer integer = counts.get(key);
				if (integer == null) {
					counts.put(key, Integer.valueOf(1));
				} else {
					counts.put(key, Integer.valueOf(integer.intValue() + 1));
				}
			}
		}

		List<String> sortedKeys = Ordering.natural()
				.onResultOf(Functions.forMap(counts))
				.immutableSortedCopy(counts.keySet());
		for (int j = 1; j <= 10 && j < sortedKeys.size(); j++) {
			String queryString = sortedKeys.get(sortedKeys.size() - j);
			String[] split = queryString.split(separatorToken);
			LOG.debug(
					"Top 10 filters are sortedKeys, FacetCode: {}, FacetValue:{}, Count:{}",
					new Object[] { split[0], split[1], counts.get(queryString) });
			topFacetFilters.add(split[1]);
		}
		LOG.debug("Checking findTopTenSearchFiltersForLastAnHourUsingRangeScan done!");
		return topFacetFilters;
	}
	
	@Override
	public int numberOfTimesAFacetFilterClickedInLastAnHour(final String columnName, final String columnValue) {
		Scan scan = new Scan();
		scan.addColumn(HbaseJsonEventSerializer.COLUMFAMILY_FILTERS_BYTES,
				Bytes.toBytes(columnName));
		Filter filter = new SingleColumnValueFilter(HbaseJsonEventSerializer.COLUMFAMILY_FILTERS_BYTES,
				Bytes.toBytes(columnName), CompareOp.EQUAL, Bytes.toBytes(columnValue));
		scan.setFilter(filter);
		DateTime dateTime = new DateTime();
		try {
			scan.setTimeRange(dateTime.minusHours(1).getMillis(), dateTime.getMillis());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		int count =
		hbaseTemplate.find("searchclicks", scan, new RowMapper<String>() {
			@Override
			public String mapRow(Result result, int rowNum) throws Exception {
				byte[] value = result.getValue(
						HbaseJsonEventSerializer.COLUMFAMILY_FILTERS_BYTES,
						Bytes.toBytes(columnName));
				if (value != null) {
					String facetValue = new String(value);
					LOG.debug("Facet field: {} and Facet Value: {}",
							new Object[] { columnName, facetValue });
				}
				return null;
			}
		}).size();

		LOG.debug("Checking numberOfTimesAFacetFilterClickedInLastAnHour done with count:{}", count);
		return count;
	}
	
	@Override
	public List<String> getAllSearchQueryStringsByCustomerInLastOneMonth(final Long customerId) {
		Scan scan = new Scan();
		scan.addColumn(HbaseJsonEventSerializer.COLUMFAMILY_SEARCH_BYTES,
				Bytes.toBytes("customerid"));
		Filter filter = new PrefixFilter(Bytes.toBytes(customerId + "_"));
		scan.setFilter(filter);
		DateTime dateTime = new DateTime();
		try {
			scan.setTimeRange(dateTime.minusDays(30).getMillis(), dateTime.getMillis());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		List<String> rows = hbaseTemplate.find("searchclicks", scan, new RowMapper<String>() {
			@Override
			public String mapRow(Result result, int rowNum) throws Exception {
				LOG.debug("Row is: {}", new String(result.getRow()));
				byte[] value = result.getValue(
						HbaseJsonEventSerializer.COLUMFAMILY_SEARCH_BYTES,
						Bytes.toBytes("querystring"));
				String queryString = null;
				if (value != null) {
					queryString = new String(value);
					LOG.debug("Query String: {}",
							new Object[] { queryString });
				}
				return queryString;
			}
		});
		List<String> list = new ArrayList<>();
		for (String string : rows) {
			if(string !=null )
			{
				list.add(string);
			}
		}
		LOG.debug("Checking getAllSearchQueryStringsByCustomerInLastOneMonth done with list:{}", list);
		return list;
	}
}
