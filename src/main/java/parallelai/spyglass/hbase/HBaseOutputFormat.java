package parallelai.spyglass.hbase;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

/**
 * Convert Map/Reduce output and write it to an HBase table
 */
public class HBaseOutputFormat extends
FileOutputFormat<ImmutableBytesWritable, Put> {

  /** JobConf parameter that specifies the output table */
  public static final String OUTPUT_TABLE = "hbase.mapred.outputtable";
  private final Log LOG = LogFactory.getLog(HBaseOutputFormat.class);


  @Override
  @SuppressWarnings("unchecked")
  public RecordWriter getRecordWriter(FileSystem ignored,
      JobConf job, String name, Progressable progress) throws IOException {

    // expecting exactly one path

    String tableName = job.get(OUTPUT_TABLE);
    HTable table = null;
    try {
      table = new HTable(HBaseConfiguration.create(job), tableName);
    } catch(IOException e) {
      LOG.error(e);
      throw e;
    }
    table.setAutoFlush(false);
    return new HBaseRecordWriter(table);
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job)
  throws FileAlreadyExistsException, InvalidJobConfException, IOException {

    String tableName = job.get(OUTPUT_TABLE);
    if(tableName == null) {
      throw new IOException("Must specify table name");
    }
  }
}