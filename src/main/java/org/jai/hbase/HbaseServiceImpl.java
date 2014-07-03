package org.jai.hbase;

import java.io.File;
import java.io.IOException;
import java.util.List;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.jai.hadoop.HadoopClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.stereotype.Service;

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
			config.set("hbase.tmp.dir", new File("target/hbasetom").getAbsolutePath());
			config.set("hbase.rootdir", hadoopClusterService.getHDFSUri() + "/hbase");
			config.set("hbase.master.port", "44335");
			config.set("hbase.master.info.port", "44345");
			config.set("hbase.regionserver.port", "44435");
			config.set("hbase.regionserver.info.port", "44445");
//			hbase.zookeeper.peerport
//			hbase.zookeeper.leaderport
			config.set("hbase.zookeeper.property.clientPort", Integer.toString(clientPort));
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
			miniHBaseCluster.shutdown();
			miniZooKeeperCluster.shutdown();
		} catch (IOException e) {
			//Don't do anything.
//			e.printStackTrace();
		}
	}
	
	@Override
	public void setupSearchEventsTable() {
		LOG.debug("Setting up searchclicks table!");
		String tableName = "searchclicks";
		TableName name = TableName.valueOf(tableName);
		HTableDescriptor desc = new HTableDescriptor(name );
		HColumnDescriptor columnFamily = new HColumnDescriptor("event".getBytes());
		desc.addFamily(columnFamily);
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(miniHBaseCluster.getConf());
			hBaseAdmin.createTable(desc);
			hBaseAdmin.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		};
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
				p.add(Bytes.toBytes("event"),
						Bytes.toBytes("json"), body);
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
		List<String> rows = hbaseTemplate.find("searchclicks", "event",
				new RowMapper<String>() {
					@Override
					public String mapRow(Result result, int rowNum)
							throws Exception {
						return new String(result.getValue(Bytes.toBytes("event"), Bytes.toBytes("json")));
					}
				});
		for (String row : rows) {
			System.out.println("Printing searchclicks row:" + row);
		}
		LOG.debug("Checking searchclicks table content done!");
	}
	
	@Override
	public int getTotalSearchClicksCount() {
		LOG.debug("Checking searchclicks table count!");
		int totalCount = 0;
		List<String> rows = hbaseTemplate.find("searchclicks", "event",
				new RowMapper<String>() {
					@Override
					public String mapRow(Result result, int rowNum)
							throws Exception {
						//getValue(Bytes.toBytes("event"), Bytes.toBytes("json"))
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
		TableName name = TableName.valueOf(tableName);
		HTableDescriptor desc = new HTableDescriptor(name );
		HColumnDescriptor columnFamily = new HColumnDescriptor("event".getBytes());
		desc.addFamily(columnFamily);
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(miniHBaseCluster.getConf());
			hBaseAdmin.disableTable(name);
			hBaseAdmin.deleteTable(name);
			hBaseAdmin.createTable(desc);
			hBaseAdmin.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		};
		LOG.debug("Setting up searchclicks table done!");
	}

	@Override
	public void testHbaseServer() {
		LOG.debug("Testing hbase server!");
		
		String tableName = "MyTable";
		TableName name = TableName.valueOf(tableName);
		HTableDescriptor desc = new HTableDescriptor(name );
		HColumnDescriptor columnFamily = new HColumnDescriptor("SomeColumn".getBytes());
		desc.addFamily(columnFamily);
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(miniHBaseCluster.getConf());
			hBaseAdmin.createTable(desc);
			hBaseAdmin.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		};
		
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

}
