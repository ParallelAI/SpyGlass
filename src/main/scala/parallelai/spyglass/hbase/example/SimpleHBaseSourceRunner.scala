package parallelai.spyglass.hbase.example

import com.twitter.scalding.Args
import org.slf4j.LoggerFactory
import parallelai.spyglass.base.JobRunner

object SimpleHBaseSourceRunner extends App {

  val mArgs = Args(args)

  val log = LoggerFactory.getLogger(this.getClass.getName)

  log.info("Starting HBaseSource Import Process Test...")

  val start1 = System.currentTimeMillis

  try {
    JobRunner.main((classOf[SimpleHBaseSourceExample].getName :: mArgs.toList).toArray)
  } finally {
    val end = System.currentTimeMillis

    log.info("HBaseSource Import process finished successfully.")
    log.info("HBaseSource Import process : " + (end - start1) + " milliseconds to complete")
  }
}