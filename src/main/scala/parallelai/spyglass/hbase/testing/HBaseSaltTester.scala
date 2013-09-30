package parallelai.spyglass.hbase.testing

import parallelai.spyglass.base.JobBase
import parallelai.spyglass.hbase.HBaseConstants.{SplitType, SourceMode}

import com.twitter.scalding._
import parallelai.spyglass.hbase.{HBasePipeConversions, HBaseSource}
import cascading.tuple.Fields
import org.apache.log4j.{Logger, Level}
import cascading.tap.SinkMode
import cascading.pipe.Pipe
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import parallelai.spyglass.hbase.HBaseSource
import com.twitter.scalding.IterableSource
import com.twitter.scalding.TextLine

class HBaseSaltTestSetup (args: Args) extends JobBase(args) with HBasePipeConversions {

  val isDebug = args.getOrElse("debug", "false").toBoolean

  if( isDebug ) { Logger.getRootLogger.setLevel(Level.DEBUG) }

  val TABLE_SCHEMA = List('key, 'salted, 'unsalted)

  val prefix = "0123456789"

  val quorum = args("quorum")

  val stt = args("start.value").toInt
  val stp = args("stop.value").toInt

  loadRanges(stt, stp)

  def loadRanges(stt: Int, stp: Int) {
    def toIBW(pipe: Pipe, f: Fields): Pipe = {
      asList(f)
        .foldLeft(pipe){ (p, f) => {
        p.map(f.toString -> f.toString){ from: String =>
          Option(from).map(x => new ImmutableBytesWritable(Bytes.toBytes(x))).getOrElse(null)
        }}
      }
    }

    val inVals = (stt to stp).toList.map(x => ("" + (x%10) + "_" + "%010d".format(x), "" + (x%10) + "_" + "%010d".format(x), "%010d".format(x)))

//    val input = IterableSource(inVals, TABLE_SCHEMA)
//      .read
//      .write(TextLine("saltTesting/Inputs"))
//
    val fromSource = new IterableSource(inVals, TABLE_SCHEMA).read.name("source_%s_%s".format(stt, stp))

    val toSource = new HBaseSource( "_TEST.SALT.01", quorum, 'key,
      TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
      TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)), sinkMode = SinkMode.UPDATE )

    toIBW(fromSource, TABLE_SCHEMA)
      .write(toSource)

  }
}

class HBaseSaltTester (args: Args) extends JobBase(args) with HBasePipeConversions {

  val isDebug = args.getOrElse("debug", "false").toBoolean

  if( isDebug ) { Logger.getRootLogger.setLevel(Level.DEBUG) }

  val TABLE_SCHEMA = List('key, 'salted, 'unsalted)

  val prefix = "0123456789"

  val quorum = args("quorum")

  val sttKey = "0000001728"
  val stpKey = "0000003725"
  val sttKeyP = "8_0000001728"
  val stpKeyP = "5_0000003725"
  val listKey = List("0000001681", "0000001456")
  val listKeyP = List("0_0000001681", "6_0000001456")
  val noSttKey = "999999999990"
  val noStpKey = "999999999999"
  val noSttKeyP = "9_999999999990"
  val noStpKeyP = "9_999999999999"
  val noListKey = List("000000123456", "000006543210")
  val noListKeyP = List("6_000000123456", "0_000006543210")

  val splitType =  if(args.getOrElse("regional", "true").toBoolean) SplitType.REGIONAL else SplitType.GRANULAR

  val testName01 = "Scan All with NO useSalt"
  val list01 = (00000 to 99999).toList.map(x => ("" + (x%10) + "_" + "%010d".format(x), "" + (x%10) + "_" + "%010d".format(x), "%010d".format(x)))
  val hbase01 = new HBaseSource( "_TEST.SALT.01", quorum, 'key,
          TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
          sourceMode = SourceMode.SCAN_ALL,
          inputSplitType = splitType ).read
          .fromBytesWritable( TABLE_SCHEMA )
          .map(('key, 'salted, 'unsalted) -> 'testData) {x: (String, String, String) => List(x._1, x._2, x._3)}
          .project('testData)
          .write(TextLine("saltTesting/ScanAllNoSalt01"))
          .groupAll(group => group.toList[List[List[String]]]('testData -> 'testData))

  val testName02 = "Scan All with useSalt=true"
  val hbase02 = new HBaseSource( "_TEST.SALT.01", quorum, 'key,
          TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
          sourceMode = SourceMode.SCAN_ALL, useSalt = true,
          inputSplitType = splitType).read
          .fromBytesWritable( TABLE_SCHEMA )
          .map(('key, 'salted, 'unsalted) -> 'testData) {x: (String, String, String) => List(x._1, x._2, x._3)}
          .project('testData)
          .write(TextLine("saltTesting/ScanAllPlusSalt01"))
          .groupAll(group => group.toList[List[List[String]]]('testData -> 'testData))

  val testName03 = "Scan Range with NO useSalt"
  val list03 = (sttKey.toInt to stpKey.toInt).toList.map(x => ("" + (x%10) + "_" + "%010d".format(x), "" + (x%10) + "_" + "%010d".format(x), "%010d".format(x)))
  val hbase03 = new HBaseSource( "_TEST.SALT.01", quorum, 'key,
          TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
          sourceMode = SourceMode.SCAN_RANGE, startKey = sttKey, stopKey = stpKey, useSalt = true, prefixList = prefix,
          inputSplitType = splitType).read
          .fromBytesWritable(TABLE_SCHEMA )
          .map(('key, 'salted, 'unsalted) -> 'testData) {x: (String, String, String) => List(x._1, x._2, x._3)}
          .project('testData)
          .write(TextLine("saltTesting/ScanRangePlusSalt01"))
          .groupAll(group => group.toList[List[List[String]]]('testData -> 'testData))

  val testName04 = "Scan Range with useSalt=true"
  val hbase04 = new HBaseSource( "_TEST.SALT.01", quorum, 'key,
          TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
          sourceMode = SourceMode.SCAN_RANGE, startKey = sttKeyP, stopKey = stpKeyP,
          inputSplitType = splitType).read
          .fromBytesWritable(TABLE_SCHEMA )
          .map(('key, 'salted, 'unsalted) -> 'testData) {x: (String, String, String) => List(x._1, x._2, x._3)}
          .project('testData)
          .write(TextLine("saltTesting/ScanRangeNoSalt01"))
          .groupAll(group => group.toList[List[List[String]]]('testData -> 'testData))


  val testName05 = "Get List with NO useSalt"
  val list05 = listKey.map(x => x.toInt).map(x => ("" + (x%10) + "_" + "%010d".format(x), "" + (x%10) + "_" + "%010d".format(x), "%010d".format(x)))
  val hbase05 = new HBaseSource( "_TEST.SALT.01", quorum, 'key,
          TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
          sourceMode = SourceMode.GET_LIST, keyList = listKey, useSalt = true,
          inputSplitType = splitType).read
          .fromBytesWritable(TABLE_SCHEMA )
          .map(('key, 'salted, 'unsalted) -> 'testData) {x: (String, String, String) => List(x._1, x._2, x._3)}
          .project('testData)
          .write(TextLine("saltTesting/GetListPlusSalt01"))
          .groupAll(group => group.toList[List[List[String]]]('testData -> 'testData))

  val testName06 = "Get List with useSalt=true"
  val hbase06 = new HBaseSource( "_TEST.SALT.01", quorum, 'key,
          TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
          sourceMode = SourceMode.GET_LIST, keyList = listKeyP,
          inputSplitType = splitType).read
          .fromBytesWritable(TABLE_SCHEMA )
          .map(('key, 'salted, 'unsalted) -> 'testData) {x: (String, String, String) => List(x._1, x._2, x._3)}
          .project('testData)
          .write(TextLine("saltTesting/GetListNoSalt01"))
          .groupAll(group => group.toList[List[List[String]]]('testData -> 'testData))

  val testName08 = "Scan Range NO RESULTS"
  val hbase08 = new HBaseSource( "_TEST.SALT.01", quorum, 'key,
    TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
    TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
    sourceMode = SourceMode.SCAN_RANGE, startKey = noSttKey, stopKey = noStpKey, useSalt = true, prefixList = prefix,
    inputSplitType = splitType).read
    .fromBytesWritable(TABLE_SCHEMA )
    .map(('key, 'salted, 'unsalted) -> 'testData) {x: (String, String, String) => List(x._1, x._2, x._3)}
    .project('testData)
    .write(TextLine("saltTesting/ScanRangePlusSaltNoRes01"))
    .groupAll(group => group.toList[List[List[String]]]('testData -> 'testData))

  val testName09 = "Scan Range NO RESULT with useSalt=true"
  val hbase09 = new HBaseSource( "_TEST.SALT.01", quorum, 'key,
    TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
    TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
    sourceMode = SourceMode.SCAN_RANGE, startKey = noSttKeyP, stopKey = noStpKeyP,
    inputSplitType = splitType).read
    .fromBytesWritable(TABLE_SCHEMA )
    .map(('key, 'salted, 'unsalted) -> 'testData) {x: (String, String, String) => List(x._1, x._2, x._3)}
    .project('testData)
    .write(TextLine("saltTesting/ScanRangeNoSaltNoRes01"))
    .groupAll(group => group.toList[List[List[String]]]('testData -> 'testData))


  val testName10 = "Get List NO RESULT"
  val hbase10 = new HBaseSource( "_TEST.SALT.01", quorum, 'key,
    TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
    TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
    sourceMode = SourceMode.GET_LIST, keyList = noListKey, useSalt = true,
    inputSplitType = splitType).read
    .fromBytesWritable(TABLE_SCHEMA )
    .map(('key, 'salted, 'unsalted) -> 'testData) {x: (String, String, String) => List(x._1, x._2, x._3)}
    .project('testData)
    .write(TextLine("saltTesting/GetListPlusSaltNoRes01"))
    .groupAll(group => group.toList[List[List[String]]]('testData -> 'testData))

  val testName11 = "Get List NO RESULT with useSalt=true"
  val hbase11 = new HBaseSource( "_TEST.SALT.01", quorum, 'key,
    TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
    TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
    sourceMode = SourceMode.GET_LIST, keyList = noListKeyP,
    inputSplitType = splitType).read
    .fromBytesWritable(TABLE_SCHEMA )
    .map(('key, 'salted, 'unsalted) -> 'testData) {x: (String, String, String) => List(x._1, x._2, x._3)}
    .project('testData)
    .write(TextLine("saltTesting/GetListNoSaltNoRes01"))
    .groupAll(group => group.toList[List[List[String]]]('testData -> 'testData))



//  (
////      getTestResultPipe(getExpectedPipe(list01), hbase01, testName01) ++
////      getTestResultPipe(getExpectedPipe(list01), hbase02, testName02) ++
//      getTestResultPipe(getExpectedPipe(list03), hbase03, testName03) ++
//      getTestResultPipe(getExpectedPipe(list03), hbase04, testName03) ++
//      getTestResultPipe(getExpectedPipe(list05), hbase05, testName05) ++
//      getTestResultPipe(getExpectedPipe(list05), hbase06, testName06) ++
//      assertPipeIsEmpty(hbase08, testName08) ++
//      assertPipeIsEmpty(hbase09, testName09) ++
//      assertPipeIsEmpty(hbase10, testName10) ++
//      assertPipeIsEmpty(hbase11, testName11)
//    ).groupAll { group =>
//    group.sortBy('testName)
//  }
//    .write(Tsv("saltTesting/FinalTestResults"))

  /**
   * We assume the pipe is empty
   *
   * We concatenate with a header - if the resulting size is 1
   * then the original size was 0 - then the pipe was empty :)
   *
   * The result is then returned in a Pipe
   */
  def assertPipeIsEmpty ( hbasePipe : Pipe , testName:String) : Pipe = {
    val headerPipe = IterableSource(List(testName), 'testData)
    val concatenation = ( hbasePipe ++ headerPipe ).groupAll{ group =>
      group.size('size)
    }
      .project('size)

    val result =
      concatenation
        .mapTo('size -> ('testName, 'result, 'expectedData, 'testData)) { x:String => {
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
      .map(('expectedData, 'testData)->'result) { x:(String,String) =>
      if (x._1.equals(x._2))
        "Success"
      else
        "Test Failed"
    }
      .project('testName, 'result, 'expectedData, 'testData)
    results
  }

  /**
   *
   */
  def getExpectedPipe ( expectedList: List[(String,String, String)]) : Pipe = {
      IterableSource(expectedList, TABLE_SCHEMA)
        .map(('key, 'salted, 'unsalted) -> 'expectedData) {x: (String, String, String) => List(x._1, x._2, x._3)}
        .project('expectedData)
        .groupAll(group => group.toList[List[List[String]]]('expectedData -> 'expectedData))
  }

}

class HBaseSaltTestShutdown (args: Args) extends JobBase(args) with HBasePipeConversions {

  val isDebug = args.getOrElse("debug", "false").toBoolean

  if( isDebug ) { Logger.getRootLogger.setLevel(Level.DEBUG) }

  val TABLE_SCHEMA = List('key, 'salted, 'unsalted)

  val prefix = "0123456789"

  val quorum = args("quorum")

  val stt = args("start.value").toInt
  val stp = args("stop.value").toInt

  delRanges(stt, stp)

  def delRanges(stt: Int, stp: Int) {
    val inVals = (stt to stp).toList.map(x => ("" + (x%10) + "_" + "%010d".format(x), "" + (x%10) + "_" + "%010d".format(x), "%010d".format(x)))

    def toIBW(pipe: Pipe, f: Fields): Pipe = {
      asList(f)
        .foldLeft(pipe){ (p, f) => {
        p.map(f.toString -> f.toString){ from: String =>
          Option(from).map(x => new ImmutableBytesWritable(Bytes.toBytes(x))).getOrElse(null)
        }}
      }
    }

    val input = IterableSource(inVals, TABLE_SCHEMA).read

    val eraser = toIBW(input, TABLE_SCHEMA)
      .write(new HBaseSource( "_TEST.SALT.01", quorum, 'key,
        TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
        TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)), sinkMode = SinkMode.REPLACE ))
  }
}