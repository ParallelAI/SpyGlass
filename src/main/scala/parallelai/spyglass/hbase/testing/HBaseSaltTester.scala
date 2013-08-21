package parallelai.spyglass.hbase.testing

import parallelai.spyglass.base.JobBase
import parallelai.spyglass.hbase.HBaseConstants.SourceMode

import com.twitter.scalding.{IterableSource, Args, TextLine}
import parallelai.spyglass.hbase.{HBasePipeConversions, HBaseSource}
import cascading.tuple.Fields
import org.apache.log4j.{Logger, Level}
import cascading.tap.SinkMode
import cascading.pipe.Pipe
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes

class HBaseSaltTestSetup (args: Args) extends JobBase(args) with HBasePipeConversions {

  val isDebug = args.getOrElse("debug", "false").toBoolean

  if( isDebug ) { Logger.getRootLogger.setLevel(Level.DEBUG) }

  val TABLE_SCHEMA = List('key, 'salted, 'unsalted)

  val prefix = "0123456789"

  val quorum = args("quorum")

  val inVals = (00000 to 99999).toList.map(x => ("" + (x%10) + "_" + "%05d".format(x), "" + (x%10) + "_" + "%05d".format(x), "%05d".format(x)))

  def toIBW(pipe: Pipe, f: Fields): Pipe = {
    asList(f)
      .foldLeft(pipe){ (p, f) => {
      p.map(f.toString -> f.toString){ from: String =>
        Option(from).map(x => new ImmutableBytesWritable(Bytes.toBytes(x))).getOrElse(null)
      }}
    }
  }


  val input = IterableSource(inVals, TABLE_SCHEMA)
    .read
    .write(TextLine("saltTesting/Inputs"))

  val maker = toIBW(IterableSource(inVals, TABLE_SCHEMA).read, TABLE_SCHEMA)
    .write(new HBaseSource( "_TEST.SALT.01", quorum, 'key,
    TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
    TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)), sinkMode = SinkMode.UPDATE ))
}

class HBaseSaltTester (args: Args) extends JobBase(args) with HBasePipeConversions {

  val isDebug = args.getOrElse("debug", "false").toBoolean

  if( isDebug ) { Logger.getRootLogger.setLevel(Level.DEBUG) }

  val TABLE_SCHEMA = List('key, 'salted, 'unsalted)

  val prefix = "0123456789"

  val quorum = args("quorum")

  val sttKey = "01728"
  val stpKey = "01831"
  val sttKeyP = "8_01728"
  val stpKeyP = "1_01831"
  val listKey = List("01681", "01456")
  val listKeyP = List("1_01681", "6_01456")
  
  val hbase01 = new HBaseSource( "_TEST.SALT.01", quorum, 'key,
          TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
          sourceMode = SourceMode.SCAN_ALL ).read
          .fromBytesWritable( TABLE_SCHEMA )
  .write(TextLine("saltTesting/ScanAllNoSalt01"))

  val hbase02 = new HBaseSource( "_TEST.SALT.01", quorum, 'key,
          TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
          sourceMode = SourceMode.SCAN_ALL, useSalt = true ).read
        .fromBytesWritable( TABLE_SCHEMA )
  .write(TextLine("saltTesting/ScanAllPlusSalt01"))

  val hbase03 = new HBaseSource( "_TEST.SALT.01", quorum, 'key,
          TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
          sourceMode = SourceMode.SCAN_RANGE, startKey = sttKeyP, stopKey = stpKeyP ).read
        .fromBytesWritable(TABLE_SCHEMA )
  .write(TextLine("saltTesting/ScanRangeNoSalt01"))

  val hbase04 = new HBaseSource( "_TEST.SALT.01", quorum, 'key,
          TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
          sourceMode = SourceMode.SCAN_RANGE, startKey = sttKey, stopKey = stpKey, useSalt = true ).read
        .fromBytesWritable(TABLE_SCHEMA )
  .write(TextLine("saltTesting/ScanRangePlusSalt01"))

  val hbase05bytes = new HBaseSource( "_TEST.SALT.01", quorum, 'key,
          TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
          sourceMode = SourceMode.GET_LIST, keyList = listKeyP ).read
          .fromBytesWritable(TABLE_SCHEMA )
  .write(TextLine("saltTesting/GetListNoSalt01"))

  val hbase06bytes = new HBaseSource( "_TEST.SALT.01", quorum, 'key,
          TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
          sourceMode = SourceMode.GET_LIST, keyList = listKey, useSalt = true).read
          .fromBytesWritable(TABLE_SCHEMA )
  .write(TextLine("saltTesting/GetListPlusSalt01"))

  val hbase07 =
      new HBaseSource( "_TEST.SALT.03", quorum, 'key,
          TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
          sourceMode = SourceMode.SCAN_RANGE, startKey = sttKey, stopKey = stpKey, useSalt = true, prefixList = prefix )
  .read
  .fromBytesWritable( TABLE_SCHEMA )
  .write(TextLine("saltTesting/ScanRangePlusSalt10"))
  .toBytesWritable( TABLE_SCHEMA )
  .write(new HBaseSource( "_TEST.SALT.04", quorum, 'key,
          TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
          useSalt = true ))

  val hbase08 =
      new HBaseSource( "_TEST.SALT.01", quorum, 'key,
          TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
          sourceMode = SourceMode.SCAN_RANGE, startKey = sttKey, stopKey = stpKey, useSalt = true, prefixList = prefix )
  .read
  .fromBytesWritable('*)
  .write(TextLine("saltTesting/ScanRangePlusSalt03"))
}

class HBaseSaltTestShutdown (args: Args) extends JobBase(args) with HBasePipeConversions {

  val isDebug = args.getOrElse("debug", "false").toBoolean

  if( isDebug ) { Logger.getRootLogger.setLevel(Level.DEBUG) }

  val TABLE_SCHEMA = List('key, 'salted, 'unsalted)

  val prefix = "0123456789"

  val quorum = args("quorum")

  val inVals = (00000 to 99999).toList.map(x => ("" + (x%10) + "_" + "%05d".format(x), "" + (x%10) + "_" + "%05d".format(x), "%05d".format(x)))

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