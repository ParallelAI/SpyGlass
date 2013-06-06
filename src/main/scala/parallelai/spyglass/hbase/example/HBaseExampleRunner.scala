package parallelai.spyglass.hbase.example

import com.twitter.scalding.Tool
import org.joda.time.format.DateTimeFormat
import java.util.Formatter.DateTime

object HBaseExampleRunner extends App {
  val appPath = System.getenv("BIGDATA_APPCONF_PATH") 
  assert  (appPath != null, {"Environment Variable BIGDATA_APPCONF_PATH is undefined or Null"})
  println( "Application Path is [%s]".format(appPath) )
  
  val modeString = if( args.length == 0 ) { "--hdfs" } else { args(0) match {
    case "hdfs" => "--hdfs"
    case _ => "--local"
  }}
  
  println(modeString)
  
  val jobLibPath = modeString match {
    case "--hdfs" => {
      val jobLibPath = System.getenv("BIGDATA_JOB_LIB_PATH") 
      assert  (jobLibPath != null, {"Environment Variable BIGDATA_JOB_LIB_PATH is undefined or Null"})
      println( "Job Library Path Path is [%s]".format(jobLibPath) )
      jobLibPath
    }
    case _ => ""
  }
  
  val quorum = System.getenv("BIGDATA_QUORUM_NAMES")
  assert  (quorum != null, {"Environment Variable BIGDATA_QUORUM_NAMES is undefined or Null"})
  println( "Quorum is [%s]".format(quorum) )

  val output = "HBaseTest.%s.tsv"

  Tool.main(Array(classOf[HBaseExample].getName, modeString, "--app.conf.path", appPath,
    "--output", output, "--debug", "true", "--job.lib.path", jobLibPath, "--quorum", quorum ))
 
}