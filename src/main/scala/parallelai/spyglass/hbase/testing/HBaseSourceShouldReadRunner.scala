package parallelai.spyglass.hbase.testing

import parallelai.spyglass.base.JobRunner

object HBaseSourceShouldReadRunner extends App {
  val appConfig = "/projects/applications.conf"
  val libPath = "/media/sf__CHANDAN_RAJAH_/Dropbox/_WORK_/_SKY_/_BIG_DATA_/_SOURCES_/big_data/commons/commons.hbase.skybase/alternateLocation"

  JobRunner.main(Array(classOf[HBaseSourceShouldRead].getName, "--hdfs", "--app.conf.path", appConfig, "--job.lib.path", libPath))
}