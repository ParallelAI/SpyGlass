package parallelai.spyglass.hbase.example

import com.twitter.scalding.Tool
import org.joda.time.format.DateTimeFormat
import java.util.Formatter.DateTime
import parallelai.spyglass.base.JobRunner
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, HTable, HConnectionManager, HBaseAdmin}
import org.apache.hadoop.hbase.io.hfile.Compression
import org.apache.hadoop.hbase.regionserver.StoreFile
import org.apache.hadoop.hbase.util.Bytes
import parallelai.spyglass.hbase.HBaseSalter

object HBaseExampleRunner extends App {
   val appPath = System.getenv("BIGDATA_APPCONF_PATH")
   assert(appPath != null, { "Environment Variable BIGDATA_APPCONF_PATH is undefined or Null" })
   println("Application Path is [%s]".format(appPath))

   val modeString = if (args.length == 0) { "--hdfs" } else {
      args(0) match {
         case "hdfs" => "--hdfs"
         case _ => "--hdfs"
      }
   }

   println(modeString)

   val jobLibPath = modeString match {
      case "--hdfs" => {
         val jobLibPath = System.getenv("BIGDATA_JOB_LIB_PATH")
         assert(jobLibPath != null, { "Environment Variable BIGDATA_JOB_LIB_PATH is undefined or Null" })
         println("Job Library Path Path is [%s]".format(jobLibPath))
         jobLibPath
      }
      case _ => ""
   }

   val quorum = System.getenv("BIGDATA_QUORUM_NAMES")
   assert(quorum != null, { "Environment Variable BIGDATA_QUORUM_NAMES is undefined or Null" })
   println("Quorum is [%s]".format(quorum))

   val output = "HBaseTest.%s"

  case class HBaseTableStore(
                              conf: Configuration,
                              quorum: String,
                              tableName: String) {

    val tableBytes = Bytes.toBytes(tableName)
    val connection = HConnectionManager.getConnection(conf)
    val maxThreads = conf.getInt("hbase.htable.threads.max", 1)

    conf.set("hbase.zookeeper.quorum", quorum)

    val htable = new HTable(HBaseConfiguration.create(conf), tableName)

    def makeN(n: Int) {
      (0 to n - 1).map(x => "%015d".format(x.toLong)).foreach(x => {
        val put = new Put(HBaseSalter.addSaltPrefix(Bytes.toBytes(x)))
        put.add(Bytes.toBytes("data"), Bytes.toBytes("data"), Bytes.toBytes(x))
      })
    }

  }

  val conf: Configuration = HBaseConfiguration.create
  HBaseTableStore(conf, quorum, "_TEST.SALT.01").makeN(100000)

   JobRunner.main(Array(classOf[HBaseExample].getName, "--hdfs", "--app.conf.path", appPath,
      "--output", output, "--debug", "true", "--job.lib.path", jobLibPath, "--quorum", quorum))


}