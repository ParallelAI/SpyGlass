package parallelai.spyglass.hbase;

import java.io.IOException;

import cascading.tap.SinkMode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;

/**
 * For writing Map/Reduce output into an HBase table
 */
public class HBaseOutputFormat extends
FileOutputFormat<ImmutableBytesWritable, Mutation> implements JobConfigurable {

  /** JobConf parameter that specifies the output table */
  public static final String OUTPUT_TABLE = "hbase.mapred.outputtable";
  private final Log LOG = LogFactory.getLog(HBaseOutputFormat.class);

  private SinkMode sinkMode = SinkMode.UPDATE;

  @Override
  public void configure(JobConf conf) {
      sinkMode = SinkMode.valueOf(
        conf.get(
            String.format(
                HBaseConstants.SINK_MODE, conf.get(HBaseOutputFormat.OUTPUT_TABLE)
            )
        )
      );
  }

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
    // TODO: Should Autoflush be set to true ???? - DONE
    table.setAutoFlush(true);
    HBaseRecordWriter recordWriter = new HBaseRecordWriter(table);
    recordWriter.setSinkMode(sinkMode);
    return recordWriter;
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job)
  throws IOException {

    String tableName = job.get(OUTPUT_TABLE);
    if(tableName == null) {
      throw new IOException("Must specify table name");
    }
  }
}