package parallelai.spyglass.hbase.example

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Level
import org.apache.log4j.Logger
import com.twitter.scalding._
import com.twitter.scalding.Args
import parallelai.spyglass.base.JobBase
import parallelai.spyglass.hbase.HBaseSource
import parallelai.spyglass.hbase.HBaseConstants.SourceMode
import org.apache.hadoop.hbase.client.Put
import parallelai.spyglass.hbase.HBaseSalter

class HBaseExample(args: Args) extends JobBase(args) {

   val isDebug: Boolean = args("debug").toBoolean

   if (isDebug) Logger.getRootLogger.setLevel(Level.DEBUG)

   val output = args("output")

   val jobConf = getJobConf()

   val quorumNames = args("quorum")

   println("Output : " + output)
   println("Quorum : " + quorumNames)

   case class HBaseTableStore(
      conf: Configuration,
      quorum: String,
      tableName: String) {

      val tableBytes = Bytes.toBytes(tableName)
      val connection = HConnectionManager.getConnection(conf)
      val maxThreads = conf.getInt("hbase.htable.threads.max", 1)

      conf.set("hbase.zookeeper.quorum", quorumNames)

      val htable = new HTable(HBaseConfiguration.create(conf), tableName)

      def makeN(n: Int) {
         (0 to n - 1).map(x => "%015d".format(x.toLong)).foreach(x => {
            val put = new Put(HBaseSalter.addSaltPrefix(Bytes.toBytes(x)))
            put.add(Bytes.toBytes("data"), Bytes.toBytes("data"), Bytes.toBytes(x))
         })
      }

   }

   HBaseTableStore(jobConf, quorumNames, "_TEST.SALT.01").makeN(100000)

   val hbs2 = new HBaseSource(
      "_TEST.SALT.01",
      quorumNames,
      'key,
      List("data"),
      List('data),
      sourceMode = SourceMode.GET_LIST, keyList = List("13914", "10687", "14897").map(x => "%015d".format(x.toLong)), useSalt = true)
      .read
      .write(Tsv(output.format("get_list")))

   val hbs3 = new HBaseSource(
      "_TEST.SALT.01",
      quorumNames,
      'key,
      List("data"),
      List('data),
      sourceMode = SourceMode.SCAN_ALL) //, stopKey = "99460693")
      .read
      .write(Tsv(output.format("scan_all")))

   val hbs4 = new HBaseSource(
      "_TEST.SALT.01",
      quorumNames,
      'key,
      List("data"),
      List('data),
      sourceMode = SourceMode.SCAN_RANGE, stopKey = "%015d".format("13914".toLong), useSalt = true)
      .read
      .write(Tsv(output.format("scan_range_to_end")))

   val hbs5 = new HBaseSource(
      "_TEST.SALT.01",
      quorumNames,
      'key,
      List("data"),
      List('data),
      sourceMode = SourceMode.SCAN_RANGE, startKey = "%015d".format("13914".toLong), useSalt = true)
      .read
      .write(Tsv(output.format("scan_range_from_start")))

   val hbs6 = new HBaseSource(
      "_TEST.SALT.01",
      quorumNames,
      'key,
      List("data"),
      List('data),
      sourceMode = SourceMode.SCAN_RANGE, startKey = "%015d".format("13914".toLong), stopKey = "%015d".format("16897".toLong), useSalt = true)
      .read
      .write(Tsv(output.format("scan_range_between")))

}