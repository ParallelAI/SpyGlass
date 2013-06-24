package parallelai.spyglass.jdbc

import com.twitter.scalding.AccessMode
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Mode
import com.twitter.scalding.Read
import com.twitter.scalding.Source
import com.twitter.scalding.Write
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tuple.Fields
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.JobConf

class JDBCSource(
    tableName: String = "tableName",
    driverName: String = "com.mysql.jdbc.Driver",
    connectionString: String = "jdbc:mysql://<hostname>:<port>/<db_name>",
    userId: String = "user",
    password: String = "password",
    columnNames: Array[String] = Array[String]("col1", "col2", "col3"),
    columnDefs: Array[String] = Array[String]("data_type", "data_type", "data_type"),
    primaryKeys: Array[String] = Array[String]("primary_key"),
    fields: Fields = new Fields("fld1", "fld2", "fld3"),
    orderBy: Array[String] = null,
    updateBy: Array[String] = null,
    updateByFields: Fields = null
  ) extends Source {

  override val hdfsScheme = new JDBCScheme(fields, columnNames, orderBy, updateByFields, updateBy)
    .asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    val jdbcScheme = hdfsScheme match {
      case jdbc: JDBCScheme => jdbc
      case _ => throw new ClassCastException("Failed casting from Scheme to JDBCScheme")
    }
    mode match {
      case hdfsMode @ Hdfs(_, _) => readOrWrite match {
        case Read => {
          val tableDesc = new TableDesc(tableName, columnNames, columnDefs, primaryKeys)
          val jdbcTap = new JDBCTap(connectionString, userId, password, driverName, tableDesc, jdbcScheme)
          jdbcTap.asInstanceOf[Tap[_,_,_]]
        }
        case Write => {

          val tableDesc = new TableDesc(tableName, columnNames, columnDefs, primaryKeys)
          val jdbcTap = new JDBCTap(connectionString, userId, password, driverName, tableDesc, jdbcScheme)
          jdbcTap.asInstanceOf[Tap[_,_,_]]
        }
      }
      case _ => super.createTap(readOrWrite)(mode)
    }
  }
}
