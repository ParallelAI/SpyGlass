package parallelai.spyglass.jdbc.testing

import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import com.twitter.scalding.Args
import com.twitter.scalding.IterableSource
import com.twitter.scalding.Tsv
import cascading.pipe.Pipe
import cascading.tuple.Fields
import parallelai.spyglass.base.JobBase
import parallelai.spyglass.jdbc.JDBCSource

/**
 * This integration-test expects some Jdbc table to exist
 * with specific data - see GenerateTestingHTables.java
 */

// https://github.com/twitter/scalding/blob/develop/scalding-core/src/test/scala/com/twitter/scalding/BlockJoinTest.scala
class JdbcSourceShouldReadWrite (args: Args) extends JobBase(args) {
  
  // Initiate logger
  private val LOG: Logger = LogManager.getLogger(this.getClass)
  
  // Set to Level.DEBUG if --debug is passed in
  val isDebug:Boolean = args.getOrElse("debug", "false").toBoolean
  if (isDebug) { 
    LOG.setLevel(Level.DEBUG)
    LOG.info("Setting logging to Level.DEBUG")
  }
  
  val url = "mysql01.domain"
  val dbName = "db"
  val tableName = "table"

  val jdbcSourceRead = new JDBCSource(
    "TABLE_01",
    "com.mysql.jdbc.Driver",
    "jdbc:mysql://localhost:3306/sky_db?zeroDateTimeBehavior=convertToNull",
    "root",
    "password",
    List("ID", "TEST_COLUMN1", "TEST_COLUMN2", "TEST_COLUMN3"),
    List("bigint(20)", "varchar(45)", "varchar(45)", "bigint(20)"),
    List("id"),
    new Fields("key", "column1", "column2", "column3"),
    null, null, null
  )

  val jdbcSourceWrite = new JDBCSource(
    "TABLE_01",
    "com.mysql.jdbc.Driver",
    "jdbc:mysql://localhost:3306/sky_db?zeroDateTimeBehavior=convertToNull",
    "root",
    "password",
    List("ID", "TEST_COLUMN1", "TEST_COLUMN2", "TEST_COLUMN3"),
    List("bigint(20)", "varchar(45)", "varchar(45)", "bigint(20)"),
    List("id"),
    new Fields("key", "column1", "column2", "column3"),
    null, null, null
  )  
    
  // -----------------------------
  // ----- Tests for TABLE_01 ----
  // -----------------------------
  val TABLE_01_SCHEMA = List('key,'column1, 'column2, 'column3)
  val tableName1 = "TABLE_01"

  // -------------------- Test 01 --------------------
  var testName01 = "Select_Test_Read_Count"
  println("---- Running : " + testName01)
  // Get everything from HBase testing table into a Pipe
  val jdbc01 = jdbcSourceRead
  		  	.read  	    
  .groupAll { group => 
  	   group.toList[String]('key -> 'key)
  	   group.toList[String]('column1 -> 'column1)
  	   group.toList[String]('column2 -> 'column2)
  	   group.toList[String]('column3 -> 'column3)
  }
  .mapTo(('key, 'column1,  'column2,  'column3) -> 'jdbcdata) { x:(String,String,String,String) =>
	     x._1 + " " + x._2 + " " + x._3 + " " + x._4
  }
  
  // Calculate expected result for Test 01
  var list01 = List(("1", "A", "X", "123"), ("2", "B", "Y", "234"),  ("3", "C", "Z", "345"))
 
  // -------------------- Test 02 --------------------
  val testName02 = "Select_Test_Read_Insert_Updated_Count"
  println("---- Running : " + testName02)
  
  // Get everything from JDBC testing table into a Pipe

  val jdbcSourceReadUpdated = new JDBCSource(
    "TABLE_02",
    "com.mysql.jdbc.Driver",
    "jdbc:mysql://localhost:3306/sky_db?zeroDateTimeBehavior=convertToNull",
    "root",
    "password",
    List("ID", "TEST_COLUMN1", "TEST_COLUMN2", "TEST_COLUMN3"),
    List("bigint(20)", "varchar(45)", "varchar(45)", "bigint(20)"),
    List("id"),
    new Fields("key", "column1", "column2", "column3"),
    null, null, null
  )  
  
  val jdbc02 = jdbcSourceReadUpdated
  .read  	    
  .groupAll { group => 
  	   group.toList[String]('key -> 'key)
  	   group.toList[String]('column1 -> 'column1)
  	   group.toList[String]('column2 -> 'column2)
  	   group.toList[String]('column3 -> 'column3)
  }
  .mapTo(('key, 'column1,  'column2,  'column3) -> 'jdbcdata) { x:(String,String,String,String) =>
	     x._1 + " " + x._2 + " " + x._3 + " " + x._4
  }
  
  // Calculate expected result for Test 02
  var list02 = List(("1", "A", "X", "123"), ("2", "B", "Y", "234"),  ("3", "C", "Z", "345"))
  
  // Store results of Scan Test 01
  (
    getTestResultPipe(getExpectedPipe(list01), jdbc01, testName01) ++
    getTestResultPipe(getExpectedPipe(list02), jdbc02, testName02)
  ).groupAll { group =>
    group.sortBy('testName)
  }
  .write(Tsv("JdbcShouldRead"))
  
  
  /**
   * We assume the pipe is empty
   * 
   * We concatenate with a header - if the resulting size is 1 
   * then the original size was 0 - then the pipe was empty :)
   * 
   * The result is then returned in a Pipe
   */
  def assertPipeIsEmpty ( jdbcPipe : Pipe , testName:String) : Pipe = { 
    val headerPipe = IterableSource(List(testName), 'jdbcdata)
    val concatenation = ( jdbcPipe ++ headerPipe ).groupAll{ group =>
      group.size('size)
    }
    .project('size)
    
    val result = 
      concatenation
      .mapTo('size -> ('testName, 'result, 'expecteddata, 'jdbcdata)) { x:String => { 
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
   * realJdbcPipe should have a column 'jdbcdata
   */
  def getTestResultPipe ( expectedPipe:Pipe , realJdbcPipe:Pipe, testName: String ): Pipe = {
	val results = expectedPipe.insert('testName , testName)
                 .joinWithTiny('testName -> 'testName, realJdbcPipe.insert('testName , testName))
    .map(('expecteddata, 'jdbcdata)->'result) { x:(String,String) =>
      	//println(x._1 + " === " + x._2)
        if (x._1.equals(x._2)) 
           "Success"
        else
           "Test Failed"
    }
    .project('testName, 'result, 'expecteddata, 'jdbcdata)
    results
  }

  /** 
   *  
   */
  def getExpectedPipe ( expectedList: List[(String,String,String,String)]) : Pipe = {
    
    val expectedPipe = 
      IterableSource(expectedList, TABLE_01_SCHEMA)
      .groupAll { group =>
        group.toList[String]('key -> 'key)
        group.toList[String]('column1 -> 'column1)
        group.toList[String]('column2 -> 'column2)
        group.toList[String]('column3 -> 'column3)
        
      }
      .mapTo(('*) -> 'expecteddata) { x:(String,String,String,String) =>
         x._1 + " " + x._2 + " " + x._3 + " " + x._4
      }
   expectedPipe
  }
  
}
