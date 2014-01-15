package parallelai.spyglass.hbase;

import java.util.Map;
import java.util.List;
import java.util.LinkedList;
import java.io.IOException;

import cascading.tap.SinkMode;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

/**
 * Convert Reduce output (key, value) to (HStoreKey, KeyedDataArrayWritable)
 * and write to an HBase table
 */
public class HBaseRecordWriter
  implements RecordWriter<ImmutableBytesWritable, Mutation> {
  private HTable m_table;
  private SinkMode m_sinkMode = SinkMode.UPDATE;
  private long deletesBufferSize;
  private List<Delete> deletesBuffer = new LinkedList<Delete>();
  private long currentDeletesBufferSize = 0; // in bytes

  /**
   * Instantiate a TableRecordWriter with the HBase HClient for writing.
   *
   * @param table
   */
  public HBaseRecordWriter(HTable table) {
    m_table = table;
    deletesBufferSize = table.getConfiguration().getLong("spyglass.deletes.buffer", 524288);
  }

  public void close(Reporter reporter)
    throws IOException {
    while (currentDeletesBufferSize > 0) {
      flushDeletes();
    }
    m_table.close();
  }

  public void setSinkMode(SinkMode sinkMode) {
    m_sinkMode = sinkMode;
  }

  public void write(ImmutableBytesWritable key,
      Mutation value) throws IOException {
    switch(m_sinkMode) {
    case UPDATE:
      if (value instanceof Put) {
        m_table.put(new Put((Put) value));
      } else if (value instanceof Delete) {
        doDelete((Delete) value);
      } else {
        throw new RuntimeException("unsupported mutation"); // currently append is not supported
      }
      break;
      
    case REPLACE:
      doDelete(new Delete(value.getRow()));
      break;
      
    default:
      throw new IOException("Unknown Sink Mode : " + m_sinkMode);
    }
  }

  private void doDelete(Delete delete) throws IOException {
    currentDeletesBufferSize += heapSizeOfDelete(delete); // currentDeletesBufferSize += delete.heapSize();
    deletesBuffer.add(new Delete((Delete) delete));
    while (currentDeletesBufferSize > deletesBufferSize) {
      flushDeletes();
    }
  }

  private void flushDeletes() throws IOException {
    try {
      m_table.delete(deletesBuffer); // successfull deletes are removed from deletesBuffer
    } finally {
      currentDeletesBufferSize = 0;
      for (Delete delete: deletesBuffer) {
        currentDeletesBufferSize += heapSizeOfDelete(delete); // currentDeletesBufferSize += delete.heapSize();
      }
    }
  }
  
  // this all goes away in newer hbase version where delete has a heapSize
  private static long heapSizeOfDelete(Delete delete) {
    long heapsize = ClassSize.align(
      // This
      ClassSize.OBJECT +
      // row + OperationWithAttributes.attributes
      2 * ClassSize.REFERENCE +
      // Timestamp
      1 * Bytes.SIZEOF_LONG +
      // durability
      ClassSize.REFERENCE +
      // familyMap
      ClassSize.REFERENCE +
      // familyMap
      ClassSize.TREEMAP);

    // Adding row
    heapsize += ClassSize.align(ClassSize.ARRAY + delete.getRow().length);

    heapsize += ClassSize.align(delete.getFamilyMap().size() * ClassSize.MAP_ENTRY);
    for(Map.Entry<byte [], List<KeyValue>> entry : delete.getFamilyMap().entrySet()) {
      //Adding key overhead
      heapsize += ClassSize.align(ClassSize.ARRAY + entry.getKey().length);

      //This part is kinds tricky since the JVM can reuse references if you
      //store the same value, but have a good match with SizeOf at the moment
      //Adding value overhead
      heapsize += ClassSize.align(ClassSize.ARRAYLIST);
      int size = entry.getValue().size();
      heapsize += ClassSize.align(ClassSize.ARRAY + size * ClassSize.REFERENCE);
      for(KeyValue kv : entry.getValue()) {
        heapsize += kv.heapSize();
      }
    }
    return heapsize;
  }
}
