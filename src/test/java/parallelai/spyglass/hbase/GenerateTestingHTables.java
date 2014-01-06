package parallelai.spyglass.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import junit.framework.Assert;
import org.junit.Test;

/**
 * Class generates TWO tables in HBase 'TABLE_01' and 'TABLE_02'
 * 
 * Those tables are used by the 'integration-testing' of HBaseSource
 * in file HBaseSourceShouldRead.scala
 * 
 * Run with:
 * mvn -Dtest=parallelai.spyglass.hbase.skybase.GenerateTestingHTables test
 * 
 * @author Antwnis@gmail.com
 */
public class GenerateTestingHTables {

	private static Configuration config = HBaseConfiguration.create();
	private static final String QUORUM = "cldmgr.test.server.com";
	private static final String QUORUM_PORT = "2181";
	private static final Long STARTING_TIMESTAMP = 1260000000000L;

	public static enum TestingTable {
		TABLE_01, TABLE_02
	}

	private static final Log LOG = LogFactory.getLog(GenerateTestingHTables.class);

	@Test
	public void fakeTest() {

		// Connect to Quorum
		LOG.info("Connecting to " + QUORUM + ":" + QUORUM_PORT);
		config.clear();
		config.set("hbase.zookeeper.quorum", QUORUM);
		config.set("hbase.zookeeper.property.clientPort", QUORUM_PORT);

		// Delete test tables
		try {
			deleteTestTable(TestingTable.TABLE_01.name());
			deleteTestTable(TestingTable.TABLE_02.name());
	
			// Generate test tables
			createTestTable(TestingTable.TABLE_01);
			createTestTable(TestingTable.TABLE_02);
	
			// Populate test tables
			populateTestTable(TestingTable.TABLE_01);
			populateTestTable(TestingTable.TABLE_02);
	
			// Print content of test table
			printHTable(TestingTable.TABLE_01);
			
			// If we've reached here - the testing data are in
			Assert.assertEquals("true", "true");
		} catch (IOException e) {
			LOG.error(e.toString());
		}

	}

	private static void populateTestTable(TestingTable testingTable)
			throws IOException {
		// Load up HBase table
		HTable table = new HTable(config, testingTable.name());
		
		LOG.info("Populating table: " + testingTable.name());

		// Table_01
		if (testingTable == TestingTable.TABLE_01) {
			Put put1 = new Put("2000-01-01 10:00:10".getBytes());
			put1.add("data".getBytes(), "column1".getBytes(), STARTING_TIMESTAMP, "1".getBytes());
			Put put2 = new Put("2000-01-01 10:05:00".getBytes());
			put2.add("data".getBytes(), "column1".getBytes(), STARTING_TIMESTAMP, "2".getBytes());
			Put put3 = new Put("2000-01-01 10:10:00".getBytes());
			put3.add("data".getBytes(), "column1".getBytes(), STARTING_TIMESTAMP, "3".getBytes());
			table.put(put1);
			table.put(put2);
			table.put(put3);
		} else
		// Table_02
		if (testingTable == TestingTable.TABLE_02) {

			// 3 versions at 10 o'clock
			byte[] k1 = "2000-01-01 10:00:00".getBytes();
			Put put1 = new Put(k1);
			put1.add("data".getBytes(), "column1".getBytes(), STARTING_TIMESTAMP        , "1".getBytes());
			Put put2 = new Put(k1);
			put2.add("data".getBytes(), "column1".getBytes(), STARTING_TIMESTAMP + 1000L, "2".getBytes());
			Put put3 = new Put(k1);
			put3.add("data".getBytes(), "column1".getBytes(), STARTING_TIMESTAMP + 2000L, "3".getBytes());

			// 3 versions at 11 o'clock
			byte[] k2 = "2000-01-01 11:00:00".getBytes();
			Put put4 = new Put(k2);
			put4.add("data".getBytes(), "column1".getBytes(), STARTING_TIMESTAMP        , "4".getBytes());
			Put put5 = new Put(k2);
			put5.add("data".getBytes(), "column1".getBytes(), STARTING_TIMESTAMP + 1000L, "5".getBytes());
			Put put6 = new Put(k2);
			put6.add("data".getBytes(), "column1".getBytes(), STARTING_TIMESTAMP + 2000L, "6".getBytes());

			// Generate list of puts
			List<Put> puts = new ArrayList<Put>();
			
			puts.add(put1);
			puts.add(put2);
			puts.add(put3);
			puts.add(put4);
			puts.add(put5);
			puts.add(put6);
			
			table.put(puts);
		}

		table.close();
	}

	private static void createTestTable(TestingTable testingTable)
			throws IOException {

		// Reset configuration
		config.clear();
		config.set("hbase.zookeeper.quorum", QUORUM);
		config.set("hbase.zookeeper.property.clientPort", QUORUM_PORT);

		HBaseAdmin hbase = new HBaseAdmin(config);

		// Get and set the name of the new table
		String tableName = testingTable.name();
		HTableDescriptor newTable = new HTableDescriptor(tableName);

		// Table1
		if (testingTable == TestingTable.TABLE_01) {
			HColumnDescriptor meta = new HColumnDescriptor("data");
			meta.setMaxVersions(3)
 		        .setCompressionType(Compression.Algorithm.NONE)
			    .setInMemory(HColumnDescriptor.DEFAULT_IN_MEMORY)
				.setBlockCacheEnabled(HColumnDescriptor.DEFAULT_BLOCKCACHE)
				.setTimeToLive(HColumnDescriptor.DEFAULT_TTL)
				.setBloomFilterType(StoreFile.BloomType.NONE);

			newTable.addFamily(meta);
			// Table2
		} else if (testingTable == TestingTable.TABLE_02) {
			HColumnDescriptor meta = new HColumnDescriptor("data".getBytes());
			meta.setMaxVersions(3)
		        .setCompressionType(Compression.Algorithm.NONE)
		        .setInMemory(HColumnDescriptor.DEFAULT_IN_MEMORY)
			    .setBlockCacheEnabled(HColumnDescriptor.DEFAULT_BLOCKCACHE)
			    .setTimeToLive(HColumnDescriptor.DEFAULT_TTL)
			    .setBloomFilterType(StoreFile.BloomType.NONE);

//			HColumnDescriptor prefix = new HColumnDescriptor("account".getBytes());
//			newTable.addFamily(prefix);
			newTable.addFamily(meta);
			LOG.info("scan 'TABLE_02' , { VERSIONS => 3 }");
		}

		try {
			LOG.info("Creating table " + tableName);
			hbase.createTable(newTable);
		} catch (TableExistsException et) {
			LOG.error("TableExistsException for table: " + tableName);
			LOG.debug(et.toString());
		} catch (IOException e) {
			LOG.error("IOException: " + e.toString());
		}

		hbase.close();
	}

	/**
	 * Method to disable and delete HBase Tables i.e. "int-test-01"
	 */
	private static void deleteTestTable(String tableName) throws IOException {

		// Reset configuration
		config.clear();
		config.set("hbase.zookeeper.quorum", QUORUM);
		config.set("hbase.zookeeper.property.clientPort", QUORUM_PORT);

		HBaseAdmin hbase = new HBaseAdmin(config);

		if (hbase.tableExists(tableName)) {
			LOG.info("Table: " + tableName + " exists.");
			hbase.disableTable(tableName);
			hbase.deleteTable(tableName);
			LOG.info("Table: " + tableName + " disabled and deleted.");
		} else {
			LOG.info("Table: " + tableName + " does not exist.");
		}

		hbase.close();
	}

	/**
	 * Method to print-out an HTable
	 */
	private static void printHTable(TestingTable testingTable)
			throws IOException {

		HTable table = new HTable(config, testingTable.name());

		Scan s = new Scan();
		// Let scanner know which columns we are interested in
		ResultScanner scanner = table.getScanner(s);

		LOG.info("Printing HTable: " + Bytes.toString(table.getTableName()));

		try {
			// Iterate results
			for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
				String key = Bytes.toString(rr.getRow());
				Iterator<KeyValue> iter = rr.list().iterator();

				String header = "Key:\t";
				String data = key + "\t";

				while (iter.hasNext()) {
					KeyValue kv = iter.next();
					header += Bytes.toString(kv.getFamily()) + ":"
							+ Bytes.toString(kv.getQualifier()) + "\t";
					data += Bytes.toString(kv.getValue()) + "\t";
				}

				LOG.info(header);
				LOG.info(data);
			}
			System.out.println();
		} finally {
			// Make sure you close your scanners when you are done!
			// Thats why we have it inside a try/finally clause
			scanner.close();
			table.close();
		}
	}

}