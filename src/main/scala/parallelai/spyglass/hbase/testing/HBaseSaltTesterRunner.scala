package parallelai.spyglass.hbase.testing

import parallelai.spyglass.base.JobRunner
import com.twitter.scalding.Args
import org.apache.log4j.{Level, Logger}

object HBaseSaltTesterRunner extends App {
  
//  if( args.length < 2 ) { throw new Exception("Not enough Args")}
  
  val appPath = System.getenv("BIGDATA_APPCONF_PATH") 
  assert  (appPath != null, {"Environment Variable BIGDATA_APPCONF_PATH is undefined or Null"})
  println( "Application Path is [%s]".format(appPath) )
  
  val jobLibPath = System.getenv("BIGDATA_JOB_LIB_PATH") 
  assert  (jobLibPath != null, {"Environment Variable BIGDATA_JOB_LIB_PATH is undefined or Null"})
  println( "Job Library Path Path is [%s]".format(jobLibPath) )

  val quorum = System.getenv("BIGDATA_QUORUM_NAMES")
  assert  (quorum != null, {"Environment Variable BIGDATA_QUORUM_NAMES is undefined or Null"})
  println( "Quorum is [%s]".format(quorum) )

  val mArgs = Args(args) // get ("make-data")

  val make = mArgs.getOrElse("make.data", "false").toBoolean
  val test = mArgs.getOrElse("test.data", "false").toBoolean
  val delete = mArgs.getOrElse("delete.data", "false").toBoolean

  val isDebug = mArgs.getOrElse("debug", "false").toBoolean

  if( isDebug ) { Logger.getRootLogger.setLevel(Level.DEBUG) }


  if( make ) {
    JobRunner.main((List(classOf[HBaseSaltTestSetup].getName,
      "--hdfs",
      "--app.conf.path", appPath,
      "--job.lib.path", jobLibPath,
      "--quorum", quorum,
      "--debug", isDebug.toString
    ) ::: mArgs.toList).toArray)
  }

  if( test ) {
    JobRunner.main((List(classOf[HBaseSaltTester].getName,
        "--hdfs",
        "--app.conf.path", appPath,
        "--job.lib.path", jobLibPath,
        "--quorum", quorum,
        "--debug", isDebug.toString,
        "--regional", mArgs.getOrElse("regional", "false")
    )::: mArgs.toList).toArray)
  }

  if( delete ) {
    JobRunner.main((List(classOf[HBaseSaltTestShutdown].getName,
      "--hdfs",
      "--app.conf.path", appPath,
      "--job.lib.path", jobLibPath,
      "--quorum", quorum,
      "--debug", isDebug.toString
    )::: mArgs.toList).toArray)
  }
}