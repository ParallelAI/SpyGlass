//package parallelai.spyglass.hbase
//
//import cascading.pipe.Pipe
//import cascading.pipe.assembly.Coerce
//import cascading.scheme.Scheme
//import cascading.tap.{ Tap, SinkMode }
//import cascading.tuple.Fields
//import org.apache.hadoop.mapred.{ RecordReader, OutputCollector, JobConf }
//import org.apache.hadoop.hbase.util.Bytes
//import scala.collection.JavaConversions._
//import scala.collection.mutable.WrappedArray
//import com.twitter.scalding._
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.client.Scan
//import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
//import org.apache.hadoop.hbase.util.Base64
//import java.io.ByteArrayOutputStream
//import java.io.DataOutputStream
//
//object HBaseRawSource {
//	/**
//	 * Converts a scan object to a base64 string that can be passed to HBaseRawSource
//	 * @param scan
//	 * @return base64 string representation
//	 */
//	def convertScanToString(scan: Scan) = {
//		val out = new ByteArrayOutputStream();
//		val dos = new DataOutputStream(out);
//		scan.write(dos);
//		Base64.encodeBytes(out.toByteArray());
//	}
//}
//
//
///**
// * @author Rotem Hermon
// *
// * HBaseRawSource is a scalding source that passes the original row (Result) object to the
// * mapper for customized processing.
// *
// * @param	tableName	The name of the HBase table to read
// * @param	quorumNames	HBase quorum
// * @param	familyNames	Column families to get (source, if null will get all) or update to (sink)
// * @param	writeNulls	Should the sink write null values. default = true. If false, null columns will not be written
// * @param	base64Scan	An optional base64 encoded scan object
// * @param	sinkMode	If REPLACE the output table will be deleted before writing to
// *
// */
//class HBaseRawSource(
//	tableName: String,
//	quorumNames: String = "localhost",
//	familyNames: Array[String],
//	writeNulls: Boolean = true,
//	base64Scan: String = null,
//	sinkMode: SinkMode = null) extends Source {
//
//	override val hdfsScheme = new HBaseRawScheme(familyNames, writeNulls)
//		.asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]
//
//	override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
//		val hBaseScheme = hdfsScheme match {
//			case hbase: HBaseRawScheme => hbase
//			case _ => throw new ClassCastException("Failed casting from Scheme to HBaseRawScheme")
//		}
//		mode match {
//			case hdfsMode @ Hdfs(_, _) => readOrWrite match {
//				case Read => {
//					new HBaseRawTap(quorumNames, tableName, hBaseScheme, base64Scan, sinkMode match {
//						case null => SinkMode.KEEP
//						case _ => sinkMode
//					}).asInstanceOf[Tap[_, _, _]]
//				}
//				case Write => {
//					new HBaseRawTap(quorumNames, tableName, hBaseScheme, base64Scan, sinkMode match {
//						case null => SinkMode.UPDATE
//						case _ => sinkMode
//					}).asInstanceOf[Tap[_, _, _]]
//				}
//			}
//			case _ => super.createTap(readOrWrite)(mode)
//		}
//	}
//}
