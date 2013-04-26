SpyGlass
========

Cascading and Scalding wrapper for HBase with advanced read features

Building
========

	$ mvn clean install -U
	
	Requires Maven 3.x.x


Example
=======

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
	
	class HBaseExample(args: Args) extends JobBase(args) {
	
	  val isDebug: Boolean = args("debug").toBoolean
	
	  if (isDebug) Logger.getRootLogger().setLevel(Level.DEBUG)
	
	  val output = args("output")
	
	  println(output)
	
	  val jobConf = getJobConf
	
	  val quorumNames = "cldmgr.prod.bigdata.bskyb.com:2181"
	
	  case class HBaseTableStore(
	      conf: Configuration,
	      quorum: String,
	      tableName: String) {
	
	    val tableBytes = Bytes.toBytes(tableName)
	    val connection = HConnectionManager.getConnection(conf)
	    val maxThreads = conf.getInt("hbase.htable.threads.max", 1)
	
	    conf.set("hbase.zookeeper.quorum", quorumNames);
	
	    val htable = new HTable(HBaseConfiguration.create(conf), tableName)
	
	  }
	
	  val hTableStore = HBaseTableStore(getJobConf, quorumNames, "skybet.test.tbet")
	
	  val hbs2 = new HBaseSource(
	    "table_name",
	    "quorum_name:2181",
	    'key,
	    Array("column_family"),
	    Array('column_name),
	    sourceMode = SourceMode.GET_LIST, keyList = List("5003914", "5000687", "5004897"))
	    .read
	    .write(Tsv(output.format("get_list")))
	
	  val hbs3 = new HBaseSource(
	    "table_name",
	    "quorum_name:2181",
	    'key,
	    Array("column_family"),
	    Array('column_name),
	    sourceMode = SourceMode.SCAN_ALL) //, stopKey = "99460693")
	    .read
	    .write(Tsv(output.format("scan_all")))
	
	  val hbs4 = new HBaseSource(
	    "table_name",
	    "quorum_name:2181",
	    'key,
	    Array("column_family"),
	    Array('column_name),
	    sourceMode = SourceMode.SCAN_RANGE, stopKey = "5003914")
	    .read
	    .write(Tsv(output.format("scan_range_to_end")))
	
	  val hbs5 = new HBaseSource(
	    "table_name",
	    "quorum_name:2181",
	    'key,
	    Array("column_family"),
	    Array('column_name),
	    sourceMode = SourceMode.SCAN_RANGE, startKey = "5003914")
	    .read
	    .write(Tsv(output.format("scan_range_from_start")))
	
	  val hbs6 = new HBaseSource(
	    "table_name",
	    "quorum_name:2181",
	    'key,
	    Array("column_family"),
	    Array('column_name),
	    sourceMode = SourceMode.SCAN_RANGE, startKey = "5003914", stopKey = "5004897")
	    .read
	    .write(Tsv(output.format("scan_range_between")))
	
	} 

