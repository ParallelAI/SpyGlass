package parallelai.spyglass.hbase.example

import com.twitter.scalding.{Tsv, Args}
import parallelai.spyglass.base.JobBase
import org.apache.log4j.{Level, Logger}
import parallelai.spyglass.hbase.{HBasePipeConversions, HBaseSource}
import parallelai.spyglass.hbase.HBaseConstants.SourceMode
import cascading.tuple.Fields

/**
  * Simple example of HBaseSource usage
  */
class SimpleHBaseSourceExample(args: Args) extends JobBase(args) with HBasePipeConversions {

   val isDebug: Boolean = args("debug").toBoolean

   if (isDebug) Logger.getRootLogger.setLevel(Level.DEBUG)

   val output = args("output")

   val hbs = new HBaseSource(
     "table_name",
     "quorum_name:2181",
     new Fields("key"),
     List("column_family"),
     List(new Fields("column_name1", "column_name2")),
     sourceMode = SourceMode.GET_LIST, keyList = List("1", "2", "3"))
     .read
     .fromBytesWritable(new Fields("key", "column_name1", "column_name2"))
     .write(Tsv(output format "get_list"))

 }
