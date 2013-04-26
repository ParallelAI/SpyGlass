package parallelai.spyglass.hbase;

import static org.apache.hadoop.hbase.mapreduce.TableRecordReaderImpl.LOG_PER_ROW_COUNT;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerCallable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.StringUtils;

import parallelai.spyglass.hbase.HBaseConstants.SourceMode;


public class HBaseRecordReader implements RecordReader<ImmutableBytesWritable, Result> {

  static final Log LOG = LogFactory.getLog(HBaseRecordReader.class);

  private byte [] startRow;
  private byte [] endRow;
  private byte [] lastSuccessfulRow;
  private TreeSet<String> keyList;
  private SourceMode sourceMode;
  private Filter trrRowFilter;
  private ResultScanner scanner;
  private HTable htable;
  private byte [][] trrInputColumns;
  private long timestamp;
  private int rowcount;
  private boolean logScannerActivity = false;
  private int logPerRowCount = 100;
  private boolean endRowInclusive = true;

  /**
   * Restart from survivable exceptions by creating a new scanner.
   *
   * @param firstRow
   * @throws IOException
   */
  public void restartRangeScan(byte[] firstRow) throws IOException {
    Scan currentScan;
    if ((endRow != null) && (endRow.length > 0)) {
      if (trrRowFilter != null) {
        Scan scan = new Scan(firstRow, (endRowInclusive ?  
            Bytes.add(endRow, new byte[] {0}) : endRow ) );
        
        TableInputFormat.addColumns(scan, trrInputColumns);
        scan.setFilter(trrRowFilter);
        scan.setCacheBlocks(false);
        this.scanner = this.htable.getScanner(scan);
        currentScan = scan;
      } else {
        LOG.debug("TIFB.restart, firstRow: " +
            Bytes.toString(firstRow) + ", endRow: " +
            Bytes.toString(endRow));
        Scan scan = new Scan(firstRow, (endRowInclusive ? Bytes.add(endRow, new byte[] {0}) : endRow ));
        TableInputFormat.addColumns(scan, trrInputColumns);
        this.scanner = this.htable.getScanner(scan);
        currentScan = scan;
      }
    } else {
      LOG.debug("TIFB.restart, firstRow: " +
          Bytes.toStringBinary(firstRow) + ", no endRow");

      Scan scan = new Scan(firstRow);
      TableInputFormat.addColumns(scan, trrInputColumns);
      scan.setFilter(trrRowFilter);
      this.scanner = this.htable.getScanner(scan);
      currentScan = scan;
    }
    if (logScannerActivity) {
      LOG.info("Current scan=" + currentScan.toString());
      timestamp = System.currentTimeMillis();
      rowcount = 0;
    }
  }

  public TreeSet<String> getKeyList() {
    return keyList;
  }

  public void setKeyList(TreeSet<String> keyList) {
    this.keyList = keyList;
  }

  public SourceMode getSourceMode() {
    return sourceMode;
  }

  public void setSourceMode(SourceMode sourceMode) {
    this.sourceMode = sourceMode;
  }

  public byte[] getEndRow() {
    return endRow;
  }
  
  public void setEndRowInclusive(boolean isInclusive) {
    endRowInclusive = isInclusive;
  }
  
  public boolean getEndRowInclusive() {
    return endRowInclusive;
  }
  
  private byte [] nextKey = null;

  /**
   * Build the scanner. Not done in constructor to allow for extension.
   *
   * @throws IOException
   */
  public void init() throws IOException {
    switch( sourceMode ) {
      case SCAN_ALL:
      case SCAN_RANGE:
        restartRangeScan(startRow);
        break;
        
      case GET_LIST:
        nextKey = Bytes.toBytes(keyList.pollFirst());
        break;
        
      default:
        throw new IOException(" Unknown source mode : " + sourceMode );
    }
  }

  byte[] getStartRow() {
    return this.startRow;
  }
  /**
   * @param htable the {@link HTable} to scan.
   */
  public void setHTable(HTable htable) {
    Configuration conf = htable.getConfiguration();
    logScannerActivity = conf.getBoolean(
      ScannerCallable.LOG_SCANNER_ACTIVITY, false);
    logPerRowCount = conf.getInt(LOG_PER_ROW_COUNT, 100);
    this.htable = htable;
  }

  /**
   * @param inputColumns the columns to be placed in {@link Result}.
   */
  public void setInputColumns(final byte [][] inputColumns) {
    this.trrInputColumns = inputColumns;
  }

  /**
   * @param startRow the first row in the split
   */
  public void setStartRow(final byte [] startRow) {
    this.startRow = startRow;
  }

  /**
   *
   * @param endRow the last row in the split
   */
  public void setEndRow(final byte [] endRow) {
    this.endRow = endRow;
  }

  /**
   * @param rowFilter the {@link Filter} to be used.
   */
  public void setRowFilter(Filter rowFilter) {
    this.trrRowFilter = rowFilter;
  }

  @Override
  public void close() {
	  if (this.scanner != null) this.scanner.close();
  }

  /**
   * @return ImmutableBytesWritable
   *
   * @see org.apache.hadoop.mapred.RecordReader#createKey()
   */
  @Override
  public ImmutableBytesWritable createKey() {
    return new ImmutableBytesWritable();
  }

  /**
   * @return RowResult
   *
   * @see org.apache.hadoop.mapred.RecordReader#createValue()
   */
  @Override
  public Result createValue() {
    return new Result();
  }

  @Override
  public long getPos() {
    // This should be the ordinal tuple in the range;
    // not clear how to calculate...
    return 0;
  }

  @Override
  public float getProgress() {
    // Depends on the total number of tuples and getPos
    return 0;
  }

  /**
   * @param key HStoreKey as input key.
   * @param value MapWritable as input value
   * @return true if there was more data
   * @throws IOException
   */
  @Override
  public boolean next(ImmutableBytesWritable key, Result value)
  throws IOException {
    
    switch(sourceMode) {
      case SCAN_ALL:
      case SCAN_RANGE:
      {
        
        Result result;
        try {
          try {
            result = this.scanner.next();
            if (logScannerActivity) {
              rowcount ++;
              if (rowcount >= logPerRowCount) {
                long now = System.currentTimeMillis();
                LOG.info("Mapper took " + (now-timestamp)
                  + "ms to process " + rowcount + " rows");
                timestamp = now;
                rowcount = 0;
              }
            }
          } catch (IOException e) {
            // try to handle all IOExceptions by restarting
            // the scanner, if the second call fails, it will be rethrown
            LOG.debug("recovered from " + StringUtils.stringifyException(e));
            if (lastSuccessfulRow == null) {
              LOG.warn("We are restarting the first next() invocation," +
                  " if your mapper has restarted a few other times like this" +
                  " then you should consider killing this job and investigate" +
                  " why it's taking so long.");
            }
            if (lastSuccessfulRow == null) {
              restartRangeScan(startRow);
            } else {
              restartRangeScan(lastSuccessfulRow);
              this.scanner.next();    // skip presumed already mapped row
            }
            result = this.scanner.next();
          }

          if (result != null && result.size() > 0) {
            key.set(result.getRow());
            lastSuccessfulRow = key.get();
            Writables.copyWritable(result, value);
            return true;
          }
          return false;
        } catch (IOException ioe) {
          if (logScannerActivity) {
            long now = System.currentTimeMillis();
            LOG.info("Mapper took " + (now-timestamp)
              + "ms to process " + rowcount + " rows");
            LOG.info(ioe);
            String lastRow = lastSuccessfulRow == null ?
              "null" : Bytes.toStringBinary(lastSuccessfulRow);
            LOG.info("lastSuccessfulRow=" + lastRow);
          }
          throw ioe;
        }
      }
      
      case GET_LIST:
      {
        Result result;
        if( nextKey != null ) {
          result = this.htable.get(new Get(nextKey));
          
          if (result != null && result.size() > 0) {
        	System.out.println("KeyList => " + keyList);
        	System.out.println("Result => " + result);
        	if (keyList != null || !keyList.isEmpty()) {
        		
        		String newKey = keyList.pollFirst();
        		System.out.println("New Key => " + newKey);
        		nextKey = (newKey == null || newKey.length() == 0) ? null : Bytes.toBytes(newKey);
        	} else {
        		nextKey = null;
        	}
            key.set(result.getRow());
            lastSuccessfulRow = key.get();
            Writables.copyWritable(result, value);
            return true;
          }
          return false;
        } else {
          return false;
        }
      }
      
      default:
        throw new IOException("Unknown source mode : " + sourceMode );
    } 
  }
}
