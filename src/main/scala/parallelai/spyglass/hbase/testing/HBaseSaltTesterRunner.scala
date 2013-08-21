package parallelai.spyglass.hbase.testing

import parallelai.spyglass.base.JobRunner
import com.twitter.scalding.Args

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

  if( make ) {
    JobRunner.main(Array(classOf[HBaseSaltTestSetup].getName,
      "--hdfs",
      "--app.conf.path", appPath,
      "--job.lib.path", jobLibPath,
      "--quorum", quorum
    ))
  }

  if( test ) {
    JobRunner.main(Array(classOf[HBaseSaltTester].getName,
        "--hdfs",
        "--app.conf.path", appPath,
        "--job.lib.path", jobLibPath,
        "--quorum", quorum
    ))
  }

  if( delete ) {
    JobRunner.main(Array(classOf[HBaseSaltTestShutdown].getName,
      "--hdfs",
      "--app.conf.path", appPath,
      "--job.lib.path", jobLibPath,
      "--quorum", quorum
    ))
  }
}