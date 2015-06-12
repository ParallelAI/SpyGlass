package parallelai.spyglass.hbase

import java.io.IOException
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import com.twitter.scalding._
import parallelai.spyglass.hbase.HBaseConstants.SplitType
import cascading.scheme.{NullScheme, Scheme}
import cascading.tap.SinkMode
import cascading.tap.Tap
import cascading.tuple.Fields
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.JobConf
import parallelai.spyglass.hbase.HBaseConstants.SourceMode
import java.io.InputStream
import java.io.OutputStream
import java.util.Properties
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Test

object Conversions {
  implicit def bytesToString(bytes: Array[Byte]): String = Bytes.toString(bytes)
  implicit def bytesToLong(bytes: Array[Byte]): Long = augmentString(bytesToString(bytes)).toLong
  implicit def ibwToString(ibw: ImmutableBytesWritable): String = bytesToString(ibw.get())
  implicit def stringToibw(s: String):ImmutableBytesWritable = new ImmutableBytesWritable(Bytes.toBytes(s))
}

case class HBaseSource(
    tableName: String = null,
    quorumNames: String = "localhost",
    keyFields: Fields = null,
    familyNames: List[String] = null,
    valueFields: List[Fields] = null,
    timestamp: Long = 0L,
    sourceMode: SourceMode = SourceMode.SCAN_ALL,
    startKey: String = null,
    stopKey: String = null,
    keyList: List[String] = null,
    versions: Int = 1,
    useSalt: Boolean = false,
    prefixList: String = null,
    autoFlush: Boolean = true,
    sinkMode: SinkMode = SinkMode.UPDATE,
    inputSplitType: SplitType = SplitType.GRANULAR
  ) extends Source {
  
  val internalScheme = new HBaseScheme(keyFields, timestamp, familyNames.toArray, valueFields.toArray)
  internalScheme.setInputSplitTye(inputSplitType)

  val hdfsScheme = internalScheme.asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]

  // To enable local mode testing
  val allFields = keyFields.append(valueFields.toArray)
  type LocalScheme = Scheme[Properties, InputStream, OutputStream, _, _]
  def localScheme = new NullScheme[Properties, InputStream, OutputStream, Any, Any] (allFields, allFields)  

  
  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    val hBaseScheme = hdfsScheme match {
      case hbase: HBaseScheme => hbase
      case _ => throw new ClassCastException("Failed casting from Scheme to HBaseScheme")
    } 
    mode match { 
      case Hdfs(_, _) => readOrWrite match {
        case Read => { 
          val hbt = new HBaseTap(quorumNames, tableName, hBaseScheme, SinkMode.KEEP)
           
          sourceMode match {
            case SourceMode.SCAN_RANGE => {
              
              hbt.setHBaseRangeParms(startKey, stopKey, useSalt, prefixList)
            }
            case SourceMode.SCAN_ALL => {
              hbt.setHBaseScanAllParms(useSalt, prefixList)
            }
            case SourceMode.GET_LIST => {
              if( keyList == null )  
                throw new IOException("Key list cannot be null when Source Mode is " + sourceMode)
              
              hbt.setHBaseListParms(keyList.toArray[String], versions, useSalt, prefixList)
            }
            case _ => throw new IOException("Unknown Source Mode (%)".format(sourceMode))
          }

          hbt.setInputSplitType(inputSplitType)
          
          hbt.asInstanceOf[Tap[_,_,_]]
        }
        case Write => {
          val hbt = new HBaseTap(quorumNames, tableName, hBaseScheme, sinkMode)
          
          hbt.setHBaseSinkParms(useSalt, autoFlush)

          hbt.asInstanceOf[Tap[_,_,_]]
        }
      }
      /**case Test(buffers) => {
        /*
        * There MUST have already been a registered sink or source in the Test mode.
        * to access this.  You must explicitly name each of your test sources in your
        * JobTest.
        */
        val buffer =
          if (readOrWrite == Write) {
            val buf = buffers(this)
            //Make sure we wipe it out:
            buf.clear()
            buf
          } else {
            // if the source is also used as a sink, we don't want its contents to get modified
            buffers(this).clone()
          }
        // TODO MemoryTap could probably be rewritten not to require localScheme, and just fields
        new MemoryTap[InputStream, OutputStream](localScheme, buffer)
      }*/      
      case Test(buffer) => readOrWrite match {
        
        case Read => { 
          val hbt = new MemoryTap[InputStream, OutputStream](localScheme, buffer.apply(this).get)
          hbt.asInstanceOf[Tap[_,_,_]]
        }
        case Write => {
          val hbt = new MemoryTap[InputStream, OutputStream](localScheme, buffer.apply(this).get)
          hbt.asInstanceOf[Tap[_,_,_]]
        }
      }      
      case _ => createEmptyTap(readOrWrite)(mode)
    }
  }
  
  
  
  def createEmptyTap(readOrWrite : AccessMode)(mode : Mode) : Tap[_,_,_] = {
    throw new RuntimeException("Source: (" + toString + ") doesn't support mode: " + mode.toString)
  }
}
