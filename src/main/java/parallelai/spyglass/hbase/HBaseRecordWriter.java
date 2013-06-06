package parallelai.spyglass.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

/**
 * Convert Reduce output (key, value) to (HStoreKey, KeyedDataArrayWritable)
 * and write to an HBase table
 */
public class HBaseRecordWriter
  implements RecordWriter<ImmutableBytesWritable, Put> {
  private HTable m_table;

  /**
   * Instantiate a TableRecordWriter with the HBase HClient for writing.
   *
   * @param table
   */
  public HBaseRecordWriter(HTable table) {
    m_table = table;
  }

  public void close(Reporter reporter)
    throws IOException {
    m_table.close();
  }

  public void write(ImmutableBytesWritable key,
      Put value) throws IOException {
    m_table.put(new Put(value));
  }
}
