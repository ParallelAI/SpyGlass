package parallelai.spyglass.hbase.testing

import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import com.twitter.scalding.Args
import com.twitter.scalding.IterableSource
import com.twitter.scalding.Tsv
import parallelai.spyglass.hbase.HBaseConstants.SourceMode
import parallelai.spyglass.hbase.HBaseSource
import parallelai.spyglass.base.JobBase
import cascading.pipe.Pipe
import parallelai.spyglass.hbase.HBasePipeConversions

/**
 * This integration-test expects some HBase table to exist
 * with specific data - see GenerateTestingHTables.java
 *
 * Keep in mind that currently:  
 * + No version support exists in Scans
 * + GET_LIST is working as a Set - Having a rowkey twice in the GET_LIST - will return in only one GET
 * 
 * ISSUES:
 *  + If Scan_Range is unordered i.e. 9 -> 1 (instead of 1 -> 9) unhandled exception is thrown:
 *  Caused by: java.lang.IllegalArgumentException: Invalid range: 9 > 11000000
 *	at org.apache.hadoop.hbase.client.HTable.getRegionsInRange(HTable.java:551)
 *  
 * @author Antwnis@gmail.com
 */

// https://github.com/twitter/scalding/blob/develop/scalding-core/src/test/scala/com/twitter/scalding/BlockJoinTest.scala
class HBaseSourceShouldRead (args: Args) extends JobBase(args) with HBasePipeConversions {
  
  // Initiate logger
  private val LOG: Logger = LogManager.getLogger(this.getClass)
  
  // Set to Level.DEBUG if --debug is passed in
  val isDebug:Boolean = args.getOrElse("debug", "false").toBoolean
  if (isDebug) { 
    LOG.setLevel(Level.DEBUG)
    LOG.info("Setting logging to Level.DEBUG")
  }
  
  // Set HBase host
  val hbaseHost = args("quorum") 

  // -----------------------------
  // ----- Tests for TABLE_01 ----
  // -----------------------------
  val TABLE_01_SCHEMA = List('key,'column1)
  val tableName1 = "TABLE_01"
  val tableName2 = "TABLE_02"

  // -------------------- Test 01 --------------------
  var testName01 = "Scan_Test_01_Huge_Key_Range"
  println("---- Running : " + testName01)
  // Get everything from HBase testing table into a Pipe
  val hbase01 = new HBaseSource(tableName1, hbaseHost, 'key,
    List("data"),
    List('column1),
    sourceMode = SourceMode.SCAN_RANGE, startKey = "2000-01-01 00:00:00", stopKey = "2000-01-02 00:00:00")
    .read
    .fromBytesWritable(
    TABLE_01_SCHEMA)
    .groupAll {
      group =>
        group.toList[String]('key -> 'key)
        group.toList[String]('column1 -> 'column1)
    }
    .mapTo(('key, 'column1) -> 'hbasedata) {
      x: (String, String) =>
        x._1 + " " + x._2
    }

  // Calculate expected result for Test 01
  var list01 = List(("2000-01-01 10:00:10", "1"),
                    ("2000-01-01 10:05:00", "2"),
                    ("2000-01-01 10:10:00", "3"))

  // -------------------- Test 02 --------------------
  val testName02 = "Scan_Test_02_Borders_Range"
  println("---- Running : " + testName02)
  // Get everything from HBase testing table into a Pipe
  val hbase02 = new HBaseSource(tableName1, hbaseHost, 'key,
    List("data"),
    List('column1),
    sourceMode = SourceMode.SCAN_RANGE, startKey = "2000-01-01 10:00:10", stopKey = "2000-01-01 10:10:00")
    .read
    .fromBytesWritable(TABLE_01_SCHEMA)
    .groupAll {
      group =>
        group.toList[String]('key -> 'key)
        group.toList[String]('column1 -> 'column1)
    }
    .mapTo(('key, 'column1) -> 'hbasedata) {
      x: (String, String) =>
        x._1 + " " + x._2
    }
  // Calculate expected result for Test 02
  var list02 = List(("2000-01-01 10:00:10", "1"), ("2000-01-01 10:05:00", "2"), ("2000-01-01 10:10:00", "3"))

  // -------------------- Test 03 --------------------
  val testName03 = "Scan_Test_03_Inner_Range"
  // Get everything from HBase testing table into a Pipe
  val hbase03 = new HBaseSource(tableName1, hbaseHost, 'key,
    List("data"),
    List('column1),
    sourceMode = SourceMode.SCAN_RANGE, startKey = "2000-01-01 10:00:55", stopKey = "2000-01-01 10:07:00")
    .read
    .fromBytesWritable(
    TABLE_01_SCHEMA)
    .groupAll {
      group =>
        group.toList[String]('key -> 'key)
        group.toList[String]('column1 -> 'column1)
    }
    .mapTo(('key, 'column1) -> 'hbasedata) {
      x: (String, String) =>
        x._1 + " " + x._2
    }

  // Calculate expected result for Test 03
  var list03 = List(("2000-01-01 10:05:00", "2"))

  // -------------------- Test 04 --------------------
  val testName04 = "Scan_Test_04_Out_Of_Range_And_Unordered"
  // Get everything from HBase testing table into a Pipe
  val hbase04 = new HBaseSource(tableName1, hbaseHost, 'key,
    List("data"),
    List('column1),
    sourceMode = SourceMode.SCAN_RANGE, startKey = "9", stopKey = "911000000")
    .read
    .fromBytesWritable(
    TABLE_01_SCHEMA)
    .groupAll {
      group =>
        group.toList[String]('key -> 'key)
        group.toList[String]('column1 -> 'column1)
    }
    .mapTo(('key, 'column1) -> 'hbasedata) {
      x: (String, String) =>
        x._1 + " " + x._2
    }
  
  // -------------------- Test 0 - TODO scan multiple versions .. --------------------
//  val testName04 = "Scan_Test_04_One_Version"
//  // Get everything from HBase testing table into a Pipe
//  val hbase04 = CommonFunctors.fromBytesWritable(
//  		new HBaseSource( tableName2, hbaseHost, 'key,  
//  		    Array("data"), 
//  		    Array('column1),
//            sourceMode = SourceMode.SCAN_RANGE, startKey = "2000-01-01 00:00:00", stopKey = "2000-01-02 00:00:00",
//            versions = 1 ) // If versions is '0' - it is regarded as '1'
//            .read  	    
//  	    , TABLE_01_SCHEMA)
//  .groupAll { group => 
//  	   group.toList[String]('key -> 'key)
//  	   group.toList[String]('column1 -> 'column1)
//  }
//  .mapTo(('key, 'column1) -> 'hbasedata) { x:(String,String) =>
//	     x._1 + " " + x._2
//  }
//  
//  // Calculate expected result for Test 04
//  var list04 = List(("",""))


  // -------------------- Test 05 --------------------
  val testName05 = "Get_Test_01_One_Existing_Some_Nonexisting_Keys_1_Versions"
  // Get everything from HBase testing table into a Pipe
  val hbase05 = new HBaseSource(tableName2, hbaseHost, 'key,
    List("data"),
    List('column1),
    sourceMode = SourceMode.GET_LIST, keyList = List("5003914", "2000-01-01 11:00:00", "5004897"),
    versions = 1)
    .read
    .fromBytesWritable(
    TABLE_01_SCHEMA)
    .groupAll {
      group =>
        group.toList[String]('key -> 'key)
        group.toList[String]('column1 -> 'column1)
    }
    .mapTo(('key, 'column1) -> 'hbasedata) {
      x: (String, String) =>
        x._1 + " " + x._2
    }

  // Calculate expected result for Test 04
  var list05 = List(("2000-01-01 11:00:00", "6"))

  // -------------------- Test 6 --------------------
  val testName06 = "Get_Test_02_One_Existing_Some_Nonexisting_Keys_2_Versions"
  val hbase06 = new HBaseSource( tableName2, hbaseHost, 'key,
    List("data"),
    List('column1),
    sourceMode = SourceMode.GET_LIST, keyList = List("a", "5003914", "2000-01-01 10:00:00"),
    versions = 2)
    .read
    .fromBytesWritable(
    TABLE_01_SCHEMA)
    .groupAll {
      group =>
        group.toList[String]('key -> 'key)
        group.toList[String]('column1 -> 'column1)
    }
    .mapTo(('key, 'column1) -> 'hbasedata) {
      x: (String, String) =>
        x._1 + " " + x._2
    }

  // Calculate expected result for Test 05
  var list06 = List(("2000-01-01 10:00:00", "3"),("2000-01-01 10:00:00","2"))

  // -------------------- Test 7 --------------------
  val testName07 = "Get_Test_03_One_Existing_Some_Nonexisting_Keys_3_Versions"
  // Get everything from HBase testing table into a Pipe
  val hbase07 = new HBaseSource( tableName2, hbaseHost, 'key,
    List("data"),
    List('column1),
    sourceMode = SourceMode.GET_LIST, keyList = List("2000", "2000-01", "2000-01-01 11:00:00", "zz"),
    versions = 3)
    .read
    .fromBytesWritable(
    TABLE_01_SCHEMA)
    .groupAll {
      group =>
        group.toList[String]('key -> 'key)
        group.toList[String]('column1 -> 'column1)
    }
    .mapTo(('key, 'column1) -> 'hbasedata) {
      x: (String, String) =>
        x._1 + " " + x._2
    }

  // Calculate expected result for Test 07
  var list07 = List(("2000-01-01 11:00:00", "6"),("2000-01-01 11:00:00","5"),("2000-01-01 11:00:00","4"))
  
  // -------------------- Test 08 --------------------
  val testName08 = "Get_Test_04_One_Existing_Some_Nonexisting_Keys_4_Versions"
  // Get everything from HBase testing table into a Pipe
  val hbase08 = new HBaseSource(tableName2, hbaseHost, 'key,
    List("data"),
    List('column1),
    sourceMode = SourceMode.GET_LIST, keyList = List("2000", "2000-01-01 11:00:00", "2000-01-01 10:00:00", "zz"),
    versions = 4)
    .read
    .fromBytesWritable(
    TABLE_01_SCHEMA)
    .groupAll {
      group =>
        group.toList[String]('key -> 'key)
        group.toList[String]('column1 -> 'column1)
    }
    .mapTo(('key, 'column1) -> 'hbasedata) {
      x: (String, String) =>
        x._1 + " " + x._2
    }

  var list08 = List(("2000-01-01 10:00:00", "3"),("2000-01-01 10:00:00","2"),("2000-01-01 10:00:00","1"),
                    ("2000-01-01 11:00:00", "6"),("2000-01-01 11:00:00","5"),("2000-01-01 11:00:00","4"))

  // -------------------- Test 09 --------------------
  val testName09 = "Get_Test_05_Get_Same_Key_Multiple_Times_4_versions"
  // Get everything from HBase testing table into a Pipe
  val hbase09 = new HBaseSource( tableName2, hbaseHost, 'key,
    List("data"),
    List('column1),
    sourceMode = SourceMode.GET_LIST, keyList = List("2000", "2000-01-01 11:00:00", "avdvf", "2000-01-01 11:00:00"),
    versions = 4)
    .read
    .fromBytesWritable(
    TABLE_01_SCHEMA)
    .groupAll {
      group =>
        group.toList[String]('key -> 'key)
        group.toList[String]('column1 -> 'column1)
    }
    .mapTo(('key, 'column1) -> 'hbasedata) {
      x: (String, String) =>
        x._1 + " " + x._2
    }

  var list09 = List(("2000-01-01 11:00:00", "6"),("2000-01-01 11:00:00","5"),("2000-01-01 11:00:00","4"))


  // -------------------- Test 10 --------------------
  val testName10 = "Get_Test_06_TestWith10000and1rowkeys"
  var bigList1:List[String] = (1 to 10000).toList.map(_.toString) 
  var bigList2:List[String] = (100001 to 200000).toList.map(_.toString)
  var bigList = ((bigList1 ::: List("2000-01-01 11:00:00")) ::: bigList2) ::: List("2000-01-01 10:00:00")   

  // Get everything from HBase testing table into a Pipe
  val hbase10 = new HBaseSource( tableName2, hbaseHost, 'key,
    List("data"),
    List('column1),
    sourceMode = SourceMode.GET_LIST, keyList = bigList,
    versions = 2) //
    .read
    .fromBytesWritable(
    TABLE_01_SCHEMA)
    .groupAll {
    group =>
        group.toList[String]('key -> 'key)
        group.toList[String]('column1 -> 'column1)
    }
    .mapTo(('key, 'column1) -> 'hbasedata) {
      x: (String, String) =>
        x._1 + " " + x._2
    }

  
  var list10 = List(("2000-01-01 10:00:00", "3"),("2000-01-01 10:00:00","2"),
		  			("2000-01-01 11:00:00", "6"),("2000-01-01 11:00:00","5")
                    )
  
  // -------------------- Test 11 --------------------
  val testName11 = "Get_Test_07_EmptyList"
  // Get everything from HBase testing table into a Pipe
  val hbase11 = new HBaseSource( tableName2, hbaseHost, 'key,
    List("data"),
    List('column1),
    sourceMode = SourceMode.GET_LIST, keyList = List(),
    versions = 1) //
    .read
    .fromBytesWritable(
    TABLE_01_SCHEMA)
    .groupAll {
      group =>
        group.toList[String]('key -> 'key)
        group.toList[String]('column1 -> 'column1)
    }
    .mapTo(('key, 'column1) -> 'hbasedata) {
      x: (String, String) =>
        x._1 + " " + x._2
    }

  
  // -------------------- Test 11 --------------------
  val testName12 = "Get_Test_08_Three_Nonexistingkeys_1_Versions"
  // Get everything from HBase testing table into a Pipe
  val hbase12 = new HBaseSource(tableName2, hbaseHost, 'key,
    List("data"),
    List('column1),
    sourceMode = SourceMode.GET_LIST, keyList = List("5003914", "5000687", "5004897"),
    versions = 1)
    .read
    .fromBytesWritable(
    TABLE_01_SCHEMA)
    .groupAll {
      group =>
        group.toList[String]('key -> 'key)
        group.toList[String]('column1 -> 'column1)
    }
    .mapTo(('key, 'column1) -> 'hbasedata) {
      x: (String, String) =>
        x._1 + " " + x._2
    }
  
  // --------------------- TEST 13 -----------------------------
  val testName13 = "Some "
  val hbase13 = new HBaseSource(tableName2, hbaseHost, 'key,
    List("data"),
    List('column1),
    sourceMode = SourceMode.SCAN_RANGE, startKey = "", stopKey = "", useSalt = true)
    .read
    .fromBytesWritable(
    TABLE_01_SCHEMA)
    .groupAll {
      group =>
        group.toList[String]('key -> 'key)
        group.toList[String]('column1 -> 'column1)
    }
    .mapTo(('key, 'column1) -> 'hbasedata) {
      x: (String, String) =>
        x._1 + " " + x._2
    }

  var list13 = List(("2000-01-01 10:00:00", "3"),("2000-01-01 10:00:00","2"),
            ("2000-01-01 11:00:00", "6"),("2000-01-01 11:00:00","5")
                    )

  
  // Store results of Scan Test 01
  (
    getTestResultPipe(getExpectedPipe(list01), hbase01, testName01) ++
    getTestResultPipe(getExpectedPipe(list02), hbase02, testName02) ++
    getTestResultPipe(getExpectedPipe(list03), hbase03, testName03) ++  
    assertPipeIsEmpty(hbase04, testName04) ++
    getTestResultPipe(getExpectedPipe(list05), hbase05, testName05) ++ 
    getTestResultPipe(getExpectedPipe(list06), hbase06, testName06) ++ 
    getTestResultPipe(getExpectedPipe(list07), hbase07, testName07) ++
    getTestResultPipe(getExpectedPipe(list08), hbase08, testName08) ++
    getTestResultPipe(getExpectedPipe(list09), hbase09, testName09) ++
    getTestResultPipe(getExpectedPipe(list10), hbase10, testName10) ++
    getTestResultPipe(getExpectedPipe(list13), hbase13, testName13) ++
    assertPipeIsEmpty(hbase11, testName11) ++
    assertPipeIsEmpty(hbase12, testName12) 
  ).groupAll { group =>
    group.sortBy('testName)
  }
  .write(Tsv("HBaseShouldRead"))
  
  
  /**
   * We assume the pipe is empty
   * 
   * We concatenate with a header - if the resulting size is 1 
   * then the original size was 0 - then the pipe was empty :)
   * 
   * The result is then returned in a Pipe
   */
  def assertPipeIsEmpty ( hbasePipe : Pipe , testName:String) : Pipe = { 
    val headerPipe = IterableSource(List(testName), 'hbasedata)
    val concatenation = ( hbasePipe ++ headerPipe ).groupAll{ group =>
      group.size('size)
    }
    .project('size)
    
    val result = 
      concatenation
      .mapTo('size -> ('testName, 'result, 'expecteddata, 'hbasedata)) { x:String => { 
	      if (x == "1") { 
	        (testName, "Success", "", "")
	      } else {
	        (testName, "Test Failed", "", "")
	      }  
    	}
      }
    
    result
  }
  
  /**
   * Methods receives 2 pipes - and projects the results of testing
   * 
   * expectedPipe  should have a column 'expecteddata
   * realHBasePipe should have a column 'hbasedata
   */
  def getTestResultPipe ( expectedPipe:Pipe , realHBasePipe:Pipe, testName: String ): Pipe = {
	val results = expectedPipe.insert('testName , testName)
                 .joinWithTiny('testName -> 'testName, realHBasePipe.insert('testName , testName))
    .map(('expecteddata, 'hbasedata)->'result) { x:(String,String) =>
        if (x._1.equals(x._2)) 
           "Success"
        else
           "Test Failed"
    }
    .project('testName, 'result, 'expecteddata, 'hbasedata)
    results
  }

  /** 
   *  
   */
  def getExpectedPipe ( expectedList: List[(String,String)]) : Pipe = {
    
    val expectedPipe = 
      IterableSource(expectedList, TABLE_01_SCHEMA)
      .groupAll { group =>
        group.toList[String]('key -> 'key)
        group.toList[String]('column1 -> 'column1)
      }
      .mapTo(('*) -> 'expecteddata) { x:(String,String) =>
         x._1 + " " + x._2
      }
   expectedPipe
  }
  
}
