package parallelai.spyglass.hbase

import cascading.scheme.Scheme
import cascading.tap.{ Tap, SinkMode }
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.mapred.{ RecordReader, OutputCollector, JobConf }
import com.twitter.scalding._
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Base64

object HBaseRawSource {
  /**
   * Writes the given scan into a Base64 encoded string.
   * Ported to Scala from private method in HBase source to obtain public access:
   * https://github.com/apache/hbase/blob/master/hbase-server/src/main/java/org/apache/hadoop/hbase/mapreduce/TableMapReduceUtil.java#L550
   * @param scan
   * @return
   */
  def convertScanToString(scan: Scan) = {
    val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan);
    Base64.encodeBytes(proto.toByteArray());
  }
}
class HBaseRawSource(
  tableName: String,
  quorumNames: String = "localhost",
  familyNames: Array[String],
  writeNulls: Boolean = true,
  base64Scan:String = null,
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
          }).asInstanceOf[Tap[_,_,_]]
        }
        case Write => {
          new HBaseRawTap(quorumNames, tableName, hBaseScheme, base64Scan, sinkMode match {
            case null => SinkMode.UPDATE
            case _ => sinkMode
          }).asInstanceOf[Tap[_,_,_]]
        }
      }
      case _ => throw new IllegalArgumentException("Illegal mode for Tap.  Expected hdfsMode.")
    }
  }
}
