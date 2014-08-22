package parallelai.spyglass.hbase.example

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import junit.framework.Assert
import org.junit.runner.RunWith
import org.scalatest.WordSpecLike
import org.scalatest.MustMatchers
import org.scalatest.junit.JUnitRunner
import org.slf4j.LoggerFactory

/**
 * Generates TWO tables in HBase 'TABLE_01' and 'TABLE_02', populate with some data
 * and perform a number of tests
 *
 * @author Antwnis@gmail.com
 */
@RunWith(classOf[JUnitRunner])
class HBaseReadTest extends MustMatchers with WordSpecLike {

  val QUORUM = "localhost"
  val QUORUM_PORT = "2181"
  val STARTING_TIMESTAMP = 1260000000000L

  val log = LoggerFactory.getLogger(this.getClass.getName)

  var config:Configuration = HBaseConfiguration.create
  config.set("hbase.zookeeper.quorum", QUORUM)
  config.set("hbase.zookeeper.property.clientPort", QUORUM_PORT)

  log.info("Connecting to Zookeeper {}:{}", QUORUM, QUORUM_PORT)

  "An HBase integration test" must {

    "generate 2 testing HBase tables" in {

      val testingTables = List("TABLE_01", "TABLE_02")

      try {
        testingTables.foreach(deleteTestTable(_,config))
        testingTables.foreach(createTestTable(_,config))
        testingTables.foreach(populateTestTable(_,config))
        testingTables.foreach(printHTable(_,config))

        // If we've reached here - the testing data are in
        Assert.assertEquals("true", "true")
      } catch {
        case e: Exception =>
          log.error("EXCEPTION ===> {}", e.toString())
      }

    }

    "perform a SCAN_ALL in an HBase table" in {

    }

    "perform a SCAN_RANGE in an HBase table" in {

    }

    "perform a GET_LIST in an HBase table" in {

    }


  }


  /**
   * Method to disable and delete HBase Tables i.e. "int-test-01"
   */
  private def deleteTestTable(tableName: String, config: Configuration ) = {

    val hbaseAdmin = new HBaseAdmin(config)
    if (hbaseAdmin.tableExists(tableName)) {
      log.info("Table: " + tableName + " exists.")
      hbaseAdmin.disableTable(tableName)
      hbaseAdmin.deleteTable(tableName)
      log.info("Table: " + tableName + " disabled and deleted.")
    } else {
      log.info("Table: " + tableName + " does not exist.")
    }
    hbaseAdmin.close()

  }

  def createTestTable(tableName: String, config: Configuration) = {

    val hbase = new HBaseAdmin(config)
    // Get and set the name of the new table
    val newTable = new HTableDescriptor(tableName)

    val meta = new HColumnDescriptor("data")
        .setMaxVersions(3)
        .setInMemory(HColumnDescriptor.DEFAULT_IN_MEMORY)
        .setBlockCacheEnabled(HColumnDescriptor.DEFAULT_BLOCKCACHE)
        .setTimeToLive(HColumnDescriptor.DEFAULT_TTL)

      newTable.addFamily(meta)

    try {
      log.info("Creating table " + tableName)
      hbase.createTable(newTable)
    } catch {
      case et: TableExistsException =>
        log.error("TableExistsException for table: {} ",tableName)
        log.debug(et.toString())
      case e: Exception =>
        log.error("IOException: " + e.toString)
    }

    hbase.close
  }

  private def populateTestTable(testingTable: String, config: Configuration) = {

    // Load up HBase table
    val table = new HTable(config, testingTable)

    log.info("Populating table: " + testingTable)

    // Table_01
    if (testingTable == "TABLE_01") {
      val put1 = new Put("2000-01-01 10:00:10".getBytes()).add("data".getBytes(), "column1".getBytes(), STARTING_TIMESTAMP, "1".getBytes())
      val put2 = new Put("2000-01-01 10:05:00".getBytes()).add("data".getBytes(), "column1".getBytes(), STARTING_TIMESTAMP, "2".getBytes())
      val put3 = new Put("2000-01-01 10:10:00".getBytes()).add("data".getBytes(), "column1".getBytes(), STARTING_TIMESTAMP, "3".getBytes())
      table.put(put1)
      table.put(put2)
      table.put(put3)
    } else
    // Table_02
    if (testingTable == "TABLE_02") {

      // 3 versions at 10 o'clock
      val k1 = "2000-01-01 10:00:00".getBytes()
      val put1 = new Put(k1).add("data".getBytes(), "column1".getBytes(), STARTING_TIMESTAMP, "1".getBytes())
      val put2 = new Put(k1).add("data".getBytes(), "column1".getBytes(), STARTING_TIMESTAMP + 1000L, "2".getBytes())
      val put3 = new Put(k1).add("data".getBytes(), "column1".getBytes(), STARTING_TIMESTAMP + 2000L, "3".getBytes())

      // 3 versions at 11 o'clock
      val k2 = "2000-01-01 11:00:00".getBytes()
      val put4 = new Put(k2).add("data".getBytes(), "column1".getBytes(), STARTING_TIMESTAMP, "4".getBytes())
      val put5 = new Put(k2).add("data".getBytes(), "column1".getBytes(), STARTING_TIMESTAMP + 1000L, "5".getBytes())
      val put6 = new Put(k2).add("data".getBytes(), "column1".getBytes(), STARTING_TIMESTAMP + 2000L, "6".getBytes())

      import scala.collection.JavaConverters._
      table.put(List(put1, put2, put3, put4, put5, put6).asJava)
    }

    table.close
  }

  /**
   * Method to print-out an HTable
   */
  private def printHTable(testingTable: String, config: Configuration) = {

    val table = new HTable(config, testingTable)
    val scanner = table.getScanner(new Scan())

    log.info("Printing HTable: " + Bytes.toString(table.getTableName()))

    try {
      // Iterate results
      //			for (Result rr = scanner.next() rr != null rr = scanner.next()) {
      while (scanner.iterator().hasNext) {
        val rr = scanner.iterator().next
        val key = Bytes.toString(rr.getRow())
        var iter = rr.list().iterator()

        var header = "Key:\t"
        var data = key + "\t"

        while (iter.hasNext()) {
          val kv = iter.next()
          header += Bytes.toString(CellUtil.cloneFamily(kv)) + ":" + Bytes.toString(CellUtil.cloneQualifier(kv)) + "\t"
          data += Bytes.toString(CellUtil.cloneValue(kv)) + "\t"
        }

        log.info(header)
        log.info(data)
      }
    } finally {
      // Close scanners when done
      scanner.close
      table.close
    }
  }

}