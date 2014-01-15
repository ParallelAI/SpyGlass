package parallelai.spyglass.hbase

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import com.twitter.scalding.Dsl._
import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.twitter.scalding.RichPipe
import com.twitter.scalding.RichFields
import org.apache.hadoop.hbase.util.Bytes
import cascading.tuple.TupleEntry

class HBasePipeWrapper (pipe: Pipe) {
    def toBytesWritable(f: Fields): Pipe = {
	  asList(f)
        .foldLeft(pipe){ (p, f) => {
	      p.map(f.toString -> f.toString){ from: String =>
            Option(from).map(x => new ImmutableBytesWritable(Bytes.toBytes(x))).getOrElse(null)
          }}
      }
    }

    def toHBaseOperation(f: Fields): Pipe = {
      asList(f)
        .foldLeft(pipe){ (p, f) => {
          p.map(f.toString -> f.toString){ from: String => from match {
            case "__DELETE_COLUMN__" => HBaseOperation.DELETE_COLUMN
            case "__DELETE_FAMILY__" => HBaseOperation.DELETE_FAMILY
            case "__DELETE_ROW__" => HBaseOperation.DELETE_ROW
            case null => HBaseOperation.NO_OP
            case x => new HBaseOperation.PutColumn(new ImmutableBytesWritable(Bytes.toBytes(x)))
          }}
        }}
    }

//   def toBytesWritable : Pipe = {
//	  asList(Fields.ALL.asInstanceOf[TupleEntry].getFields()).foldLeft(pipe){ (p, f) => {
//	    p.map(f.toString -> f.toString){ from: String => {
//	      new ImmutableBytesWritable(Bytes.toBytes(from))
//	    }}
//	  }} 
//	}

	def fromBytesWritable(f: Fields): Pipe = {
	  asList(f)
	    .foldLeft(pipe) { (p, fld) => {
	      p.map(fld.toString -> fld.toString) { from: ImmutableBytesWritable =>
            Option(from).map(x => Bytes.toString(x.get)).getOrElse(null)
          }
        }}
    }

//	def fromBytesWritable : Pipe = {
//	  asList(Fields.ALL.asInstanceOf[TupleEntry].getFields()).foldLeft(pipe) { (p, fld) =>
//	    p.map(fld.toString -> fld.toString) { from: ImmutableBytesWritable => {
//	    	Bytes.toString(from.get)
//	      }
//	    }
//	  }
//	}
}

trait HBasePipeConversions {
  implicit def pipeWrapper(pipe: Pipe) = new HBasePipeWrapper(pipe) 
}


