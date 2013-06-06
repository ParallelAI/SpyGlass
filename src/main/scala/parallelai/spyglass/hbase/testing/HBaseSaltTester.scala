package parallelai.spyglass.hbase.testing

import parallelai.spyglass.base.JobBase
import parallelai.spyglass.hbase.HBaseConstants.SourceMode;

import com.twitter.scalding.Args
import parallelai.spyglass.hbase.HBaseSource
import com.twitter.scalding.Tsv
import cascading.tuple.Fields
import com.twitter.scalding.TextLine
import org.apache.log4j.Logger
import org.apache.log4j.Level
import parallelai.spyglass.hbase.HBasePipeConversions
import cascading.pipe.Pipe

class HBaseSaltTester (args: Args) extends JobBase(args) with HBasePipeConversions {

  val isDebug = args.getOrElse("debug", "false").toBoolean
  
  if( isDebug ) { Logger.getRootLogger().setLevel(Level.DEBUG) } 
  
  val TABLE_SCHEMA = List('key, 'salted, 'unsalted)
  
  val prefix = "0123456789"
    
  val quorum = args("quorum")
  
  val hbase01 = new HBaseSource( "_TEST.SALT.01", quorum, 'key,  
          TABLE_SCHEMA.tail.map((x: Symbol) => "data").toArray, 
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)).toArray,
          sourceMode = SourceMode.SCAN_ALL ).read
          .fromBytesWritable( TABLE_SCHEMA )
  .write(TextLine("saltTesting/ScanAllNoSalt01"))

  val hbase02 = new HBaseSource( "_TEST.SALT.01", quorum, 'key,  
          TABLE_SCHEMA.tail.map((x: Symbol) => "data").toArray, 
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)).toArray,
          sourceMode = SourceMode.SCAN_ALL, useSalt = true ).read       
        .fromBytesWritable( TABLE_SCHEMA )
  .write(TextLine("saltTesting/ScanAllPlusSalt01"))

  val hbase03 = new HBaseSource( "_TEST.SALT.01", quorum, 'key,  
          TABLE_SCHEMA.tail.map((x: Symbol) => "data").toArray, 
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)).toArray,
          sourceMode = SourceMode.SCAN_RANGE, startKey = "8_1728", stopKey = "1_1831" ).read     
        .fromBytesWritable(TABLE_SCHEMA )
  .write(TextLine("saltTesting/ScanRangeNoSalt01"))

  val hbase04 = new HBaseSource( "_TEST.SALT.01", quorum, 'key,  
          TABLE_SCHEMA.tail.map((x: Symbol) => "data").toArray, 
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)).toArray,
          sourceMode = SourceMode.SCAN_RANGE, startKey = "1728", stopKey = "1831", useSalt = true ).read       
        .fromBytesWritable(TABLE_SCHEMA )
  .write(TextLine("saltTesting/ScanRangePlusSalt01"))

  val hbase05bytes = new HBaseSource( "_TEST.SALT.01", quorum, 'key,  
          TABLE_SCHEMA.tail.map((x: Symbol) => "data").toArray, 
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)).toArray,
          sourceMode = SourceMode.GET_LIST, keyList = List("1_1681", "6_1456") ).read
          
          .fromBytesWritable(TABLE_SCHEMA )
  .write(TextLine("saltTesting/GetListNoSalt01"))

  val hbase06bytes = new HBaseSource( "_TEST.SALT.01", quorum, 'key,  
          TABLE_SCHEMA.tail.map((x: Symbol) => "data").toArray, 
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)).toArray,
          sourceMode = SourceMode.GET_LIST, keyList = List("1681", "1456"), useSalt = true).read
          
          .fromBytesWritable(TABLE_SCHEMA )
  .write(TextLine("saltTesting/GetListPlusSalt01"))

  val hbase07 = 
      new HBaseSource( "_TEST.SALT.03", quorum, 'key,  
          TABLE_SCHEMA.tail.map((x: Symbol) => "data").toArray, 
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)).toArray,
          sourceMode = SourceMode.SCAN_RANGE, startKey = "11445", stopKey = "11455", useSalt = true, prefixList = prefix )
  .read
  .fromBytesWritable( TABLE_SCHEMA )
  .write(TextLine("saltTesting/ScanRangePlusSalt10"))
  .toBytesWritable( TABLE_SCHEMA )
  .write(new HBaseSource( "_TEST.SALT.04", quorum, 'key,  
          TABLE_SCHEMA.tail.map((x: Symbol) => "data").toArray, 
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)).toArray,
          useSalt = true ))

//  val hbase08 = 
//      new HBaseSource( "_TEST.SALT.01", quorum, 'key,  
//          TABLE_SCHEMA.tail.map((x: Symbol) => "data").toArray, 
//          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)).toArray,
//          sourceMode = SourceMode.SCAN_RANGE, startKey = "1445", stopKey = "1455", useSalt = true, prefixList = prefix )
//  .read
//  .fromBytesWritable('*)
//  .write(TextLine("saltTesting/ScanRangePlusSalt03"))

}