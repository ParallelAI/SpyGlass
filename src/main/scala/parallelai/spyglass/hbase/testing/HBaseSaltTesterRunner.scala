package parallelai.spyglass.hbase.testing

import parallelai.spyglass.base.JobRunner

object HBaseSaltTesterRunner extends App {
  
//  if( args.length < 2 ) { throw new Exception("Not enough Args")}
  
  val appConfig = "/home/crajah/tmp/application.conf"
  val libPath = "/home/crajah/Dropbox/_WORK_/_SKY_/_BIG_DATA_/_SOURCES_/big_data/commons/commons.hbase.skybase/alternateLocation"

  JobRunner.main(Array(classOf[HBaseSaltTester].getName, 
      "--hdfs", 
      "--app.conf.path", appConfig, 
      "--job.lib.path", libPath,
      "--debug", "true"
  ))
}