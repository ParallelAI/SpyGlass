package parallelai.spyglass.jdbc.example

import com.twitter.scalding.{Tsv, Args}
import parallelai.spyglass.base.JobBase
import org.apache.log4j.{Level, Logger}
import parallelai.spyglass.jdbc.JDBCSource
import cascading.tuple.Fields

/**
 * Simple example of JDBCSource usage
 */
class JdbcSourceExample(args: Args) extends JobBase(args) {

  val isDebug: Boolean = args("debug").toBoolean

  if (isDebug) Logger.getRootLogger.setLevel(Level.DEBUG)

  val output = args("output")

  val hbs2 = new JDBCSource(
    "db_name",
    "com.mysql.jdbc.Driver",
    "jdbc:mysql://<hostname>:<port>/<db_name>?zeroDateTimeBehavior=convertToNull",
    "user",
    "password",
    List("KEY_ID", "COL1", "COL2", "COL3"),
    List("bigint(20)", "varchar(45)", "varchar(45)", "bigint(20)"),
    List("key_id"),
    new Fields("key_id", "col1", "col2", "col3")
  ).read
    .write(Tsv(output.format("get_list")))

}
