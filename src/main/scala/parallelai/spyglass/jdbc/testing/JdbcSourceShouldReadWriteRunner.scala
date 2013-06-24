package parallelai.spyglass.jdbc.testing

import parallelai.spyglass.base.JobRunner

object JdbcSourceShouldReadWriteRunner extends App {
  val appConfig = "/projects/applications.conf"
  val libPath = "/*.jar"

  JobRunner.main(Array(classOf[JdbcSourceShouldReadWrite].getName, "--hdfs", "--app.conf.path", appConfig, "--job.lib.path", libPath))
}