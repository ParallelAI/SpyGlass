package parallelai.spyglass.hbase.example

import com.twitter.scalding.{Tsv, Args}
import parallelai.spyglass.base.JobBase
import org.apache.log4j.{Level, Logger}
import parallelai.spyglass.hbase.{HBasePipeConversions, HBaseSource}
import parallelai.spyglass.hbase.HBaseConstants.SourceMode
import cascading.tuple.Fields
import com.twitter.scalding.IterableSource

/**
  * Simple example of HBaseSource usage
  */
class SimpleHBaseSourceExample(args: Args) extends JobBase(args) with HBasePipeConversions {

   //val isDebug: Boolean = args("debug").toBoolean
   //if (isDebug) Logger.getRootLogger.setLevel(Level.DEBUG)

   val output = args("output")

   val hbsOut = new HBaseSource(
     "spyglass.hbase.test",
     "cldmgr.prod.bigdata.bskyb.com:2181",
     new Fields("key"),
     List("data", "data"),
     List(new Fields("test1", "test2")))   
   
  val data = List(
    ("100", 1, "A"),
    ("101", 2,  "B"),
    ("102" , 3 , "C"),
    ("103" , 4 , "D"),
    ("104" , 5 , "E"),
    ("104" , 6 , "F"))
   
  val testDataPipe =
    IterableSource[(String, Int, String)](data, ('key, 'test1, 'test2))
      .debug
      .toBytesWritable(List('key, 'test1, 'test2))
      
   val writer = testDataPipe   
   writer.write(hbsOut)    
    
   val hbs = new HBaseSource(
	     "spyglass.hbase.test",
	     "cldmgr.prod.bigdata.bskyb.com:2181",
	     new Fields("key"),
	     List("data", "data"),
	     List(new Fields("test1", "test2")),
	     sourceMode = SourceMode.SCAN_ALL)
	     .read
	     .fromBytesWritable(new Fields("key", "test1", "test2"))

	val fileWriter = hbs     
	fileWriter.write(Tsv("scan_all.txt"))

  
  
	
   
 }
