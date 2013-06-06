package parallelai.spyglass.hbase.testing

import parallelai.spyglass.base.JobRunner

object HBaseSourceShouldReadRunner extends App {
  val appPath = System.getenv("BIGDATA_APPCONF_PATH") 
  assert  (appPath != null, {"Environment Variable BIGDATA_APPCONF_PATH is undefined or Null"})
  println( "Application Path is [%s]".format(appPath) )
  
  val jobLibPath = System.getenv("BIGDATA_JOB_LIB_PATH") 
  assert  (jobLibPath != null, {"Environment Variable BIGDATA_JOB_LIB_PATH is undefined or Null"})
  println( "Job Library Path Path is [%s]".format(jobLibPath) )

  val quorum = System.getenv("BIGDATA_QUORUM_NAMES")
  assert  (quorum != null, {"Environment Variable BIGDATA_QUORUM_NAMES is undefined or Null"})
  println( "Quorum is [%s]".format(quorum) )

  JobRunner.main(Array(classOf[HBaseSourceShouldRead].getName, "--hdfs", "--app.conf.path", appPath, "--job.lib.path", jobLibPath, "--quorum", quorum))
}