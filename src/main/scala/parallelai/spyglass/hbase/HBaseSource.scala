package parallelai.spyglass.hbase

import java.io.IOException
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import com.twitter.scalding.AccessMode
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Mode
import com.twitter.scalding.Read
import com.twitter.scalding.Source
import com.twitter.scalding.Write
import cascading.scheme.Scheme
import cascading.tap.SinkMode
import cascading.tap.Tap
import cascading.tuple.Fields
import org.apache.hadoop.mapred.RecordReader
import scala.compat.Platform
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.JobConf
import parallelai.spyglass.hbase.HBaseConstants.SourceMode

object Conversions {
  implicit def bytesToString(bytes: Array[Byte]): String = Bytes.toString(bytes)
  implicit def bytesToLong(bytes: Array[Byte]): Long = augmentString(bytesToString(bytes)).toLong
  implicit def ibwToString(ibw: ImmutableBytesWritable): String = bytesToString(ibw.get())
  implicit def stringToibw(s: String):ImmutableBytesWritable = new ImmutableBytesWritable(Bytes.toBytes(s))
}

class HBaseSource(
    tableName: String = null,
    quorumNames: String = "localhost",
    keyFields: Fields = null,
    familyNames: Array[String] = null,
    valueFields: Array[Fields] = null,
    timestamp: Long = Platform.currentTime,
    sourceMode: SourceMode = SourceMode.SCAN_ALL,
    startKey: String = null,
    stopKey: String = null,
    keyList: List[String] = null
  ) extends Source {
    
  override val hdfsScheme = new HBaseScheme(keyFields, timestamp, familyNames, valueFields)
    .asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    val hBaseScheme = hdfsScheme match {
      case hbase: HBaseScheme => hbase
      case _ => throw new ClassCastException("Failed casting from Scheme to HBaseScheme")
    } 
    mode match { 
      case hdfsMode @ Hdfs(_, _) => readOrWrite match {
        case Read => { 
          val hbt = new HBaseTap(quorumNames, tableName, hBaseScheme, SinkMode.KEEP)
          
          sourceMode match {
            case SourceMode.SCAN_RANGE => {
              hbt.setHBaseRangeParms(startKey, stopKey)
            }
            case SourceMode.SCAN_ALL => {
              hbt.setHBaseScanAllParms()
            }
            case SourceMode.GET_LIST => {
              if( keyList == null ) 
                throw new IOException("Key list cannot be null when Source Mode is " + sourceMode)
              
              hbt.setHBaseListParms(keyList.toArray[String])
            }
            case _ => throw new IOException("Unknown Source Mode (%)".format(sourceMode))
          }
          
          hbt.asInstanceOf[Tap[_,_,_]]
        }
        case Write => {
          val hbt = new HBaseTap(quorumNames, tableName, hBaseScheme, SinkMode.UPDATE)
          
          hbt.asInstanceOf[Tap[_,_,_]]
        }
      }
      case _ => super.createTap(readOrWrite)(mode)
    }
  }
}
