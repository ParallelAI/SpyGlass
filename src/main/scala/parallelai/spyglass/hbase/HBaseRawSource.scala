package parallelai.spyglass.hbase

import cascading.scheme.Scheme
import cascading.tap.{ Tap, SinkMode }
import org.apache.hadoop.mapred.{ RecordReader, OutputCollector, JobConf }
import com.twitter.scalding._
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.protobuf.ProtobufUtil

/* I don't know why this entire file was commented out in the SpyGlass source, but it's the main thing I wanted
   So I'm pulling it in myself
 */

object HBaseRawSource {
	/**
	 * Converts a scan object to a base64 string that can be passed to HBaseRawSource
	 * @param scan
	 * @return base64 string representation
	 */
  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray())
	}
}


/**
* @author Rotem Hermon
*
* HBaseRawSource is a scalding source that passes the original row (Result) object to the
* mapper for customized processing.
*
* @param	tableName	The name of the HBase table to read
* @param	quorumNames	HBase quorum
* @param	familyNames	Column families to get (source, if null will get all) or update to (sink)
* @param	writeNulls	Should the sink write null values. default = true. If false, null columns will not be written
* @param	base64Scan	An optional base64 encoded scan object
* @param	sinkMode	If REPLACE the output table will be deleted before writing to
*
*/
class HBaseRawSource(
	tableName: String,
	quorumNames: String = "localhost",
	familyNames: Array[String],
	writeNulls: Boolean = true,
	base64Scan: String = null,
	sinkMode: SinkMode = null) extends Source {

  	val hdfsScheme = new HBaseRawScheme(familyNames, writeNulls)
		.asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]

	override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
		val hBaseScheme = hdfsScheme match {
			case hbase: HBaseRawScheme => hbase
			case _ => throw new ClassCastException("Failed casting from Scheme to HBaseRawScheme")
		}
		mode match {
			case hdfsMode @ Hdfs(_, _) => readOrWrite match {
				case Read => {
					new HBaseRawTap(quorumNames, tableName, hBaseScheme, base64Scan, sinkMode match {
						case null => SinkMode.KEEP
						case _ => sinkMode
					}).asInstanceOf[Tap[_, _, _]]
				}
				case Write => {
					new HBaseRawTap(quorumNames, tableName, hBaseScheme, base64Scan, sinkMode match {
						case null => SinkMode.UPDATE
						case _ => sinkMode
					}).asInstanceOf[Tap[_, _, _]]
				}
			}
		}
	}
}
