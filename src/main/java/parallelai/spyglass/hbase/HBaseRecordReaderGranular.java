package parallelai.spyglass.hbase;

import static org.apache.hadoop.hbase.mapreduce.TableRecordReaderImpl.LOG_PER_ROW_COUNT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
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

import org.jruby.javasupport.util.RuntimeHelpers;
import parallelai.spyglass.hbase.HBaseConstants.SourceMode;

public class HBaseRecordReaderGranular extends HBaseRecordReaderBase {

  static final Log LOG = LogFactory.getLog(HBaseRecordReaderGranular.class);

  private byte[] lastSuccessfulRow;
  private ResultScanner scanner;
  private long timestamp;
  private int rowcount = 0;

    @Override
    public String toString() {
        StringBuffer sbuf = new StringBuffer();

        sbuf.append("".format("HBaseRecordReaderRegional : startRow [%s] endRow [%s] lastRow [%s] nextKey [%s] endRowInc [%s] rowCount [%s]",
                Bytes.toString(startRow), Bytes.toString(endRow), Bytes.toString(lastSuccessfulRow), Bytes.toString(nextKey), endRowInclusive, rowcount));
        sbuf.append("".format(" sourceMode [%s] salt [%s] versions [%s] ",
                sourceMode, useSalt, versions));

        return sbuf.toString();
    }

    private final int scanCaching = 1000;


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
        Scan scan = new Scan(firstRow, (endRowInclusive ? Bytes.add(endRow,
            new byte[] { 0 }) : endRow));

        TableInputFormat.addColumns(scan, trrInputColumns);
        scan.setFilter(trrRowFilter);
        scan.setCacheBlocks(true);
        scan.setCaching(scanCaching);
        this.scanner = this.htable.getScanner(scan);
        currentScan = scan;
      } else {
        LOG.debug("TIFB.restart, firstRow: " + Bytes.toString(firstRow)
            + ", endRow: " + Bytes.toString(endRow));
        Scan scan = new Scan(firstRow, (endRowInclusive ? Bytes.add(endRow,
            new byte[] { 0 }) : endRow));
        TableInputFormat.addColumns(scan, trrInputColumns);
          scan.setCacheBlocks(true);
          scan.setCaching(scanCaching);
        this.scanner = this.htable.getScanner(scan);
        currentScan = scan;
      }
    } else {
      LOG.debug("TIFB.restart, firstRow: " + Bytes.toStringBinary(firstRow)
          + ", no endRow");

      Scan scan = new Scan(firstRow);
      TableInputFormat.addColumns(scan, trrInputColumns);
      scan.setFilter(trrRowFilter);
      scan.setCacheBlocks(true);
        scan.setCaching(scanCaching);
      this.scanner = this.htable.getScanner(scan);
      currentScan = scan;
    }
    if (logScannerActivity) {
      LOG.debug("Current scan=" + currentScan.toString());
      timestamp = System.currentTimeMillis();
      rowcount = 0;
    }
  }


  private byte[] nextKey = null;
  private Vector<List<KeyValue>> resultVector = null;
  Map<Long, List<KeyValue>> keyValueMap = null;

  /**
   * Build the scanner. Not done in constructor to allow for extension.
   * 
   * @throws IOException
   */
  public void init() throws IOException {
    switch (sourceMode) {
    case SCAN_ALL:
    case SCAN_RANGE:
      restartRangeScan(startRow);
      break;

    case GET_LIST:
      nextKey = Bytes.toBytes(keyList.pollFirst());
      break;

    default:
      throw new IOException(" Unknown source mode : " + sourceMode);
    }
  }

  @Override
  public void close() {
    if (this.scanner != null)
      this.scanner.close();
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
    switch(sourceMode) {
        case GET_LIST:
            long posGet = (keyList != null ) ? 0 : initialNoOfKeys - keyList.size() ;
            return posGet;

        case SCAN_ALL:
        case SCAN_RANGE:
            long posScan = (noOfLogCount * logPerRowCount) + rowcount;
            return posScan;

        default:
            return 0;
    }
  }

  @Override
  public float getProgress() {
    // Depends on the total number of tuples and getPos
    switch(sourceMode) {
        case GET_LIST:
            float progGet = ((initialNoOfKeys == 0) ? 0 : (getPos() / initialNoOfKeys));
            return progGet;

        case SCAN_ALL:
        case SCAN_RANGE:
            float progScan = (getPos() / (getPos() + 10000));
            return progScan;

        default:
            return 0;
    }
  }

  /**
   * @param key
   *          HStoreKey as input key.
   * @param value
   *          MapWritable as input value
   * @return true if there was more data
   * @throws IOException
   */
  @Override
  public boolean next(ImmutableBytesWritable key, Result value)
      throws IOException {

    switch (sourceMode) {
    case SCAN_ALL:
    case SCAN_RANGE: {

      Result result;
      try {
        try {
          result = this.scanner.next();
            rowcount++;
            if (rowcount >= logPerRowCount) {
                long now = System.currentTimeMillis();
                timestamp = now;
                noOfLogCount ++;
                rowcount = 0;
            }

          if (logScannerActivity) {
              long now = System.currentTimeMillis();
              LOG.debug("Mapper took " + (now - timestamp) + "ms to process "
                      + rowcount + " rows");
          }
        } catch (IOException e) {
          // try to handle all IOExceptions by restarting
          // the scanner, if the second call fails, it will be rethrown
          LOG.debug("recovered from " + StringUtils.stringifyException(e));
          if (lastSuccessfulRow == null) {
            LOG.warn("We are restarting the first next() invocation,"
                + " if your mapper has restarted a few other times like this"
                + " then you should consider killing this job and investigate"
                + " why it's taking so long.");
          }
          if (lastSuccessfulRow == null) {
            restartRangeScan(startRow);
          } else {
            restartRangeScan(lastSuccessfulRow);
            this.scanner.next(); // skip presumed already mapped row
          }
          result = this.scanner.next();
        }

        if (result != null && result.size() > 0) {
          if( useSalt) {
            key.set( HBaseSalter.delSaltPrefix(result.getRow()));
          } else {
            key.set(result.getRow());
          }
          
          lastSuccessfulRow = key.get();
          Writables.copyWritable(result, value);
          return true;
        }
        return false;
      } catch (IOException ioe) {
        if (logScannerActivity) {
          long now = System.currentTimeMillis();
          LOG.debug("Mapper took " + (now - timestamp) + "ms to process "
              + rowcount + " rows");
          LOG.debug(ioe);
          String lastRow = lastSuccessfulRow == null ? "null" : Bytes
              .toStringBinary(lastSuccessfulRow);
          LOG.debug("lastSuccessfulRow=" + lastRow);
        }
        throw ioe;
      }
    }

    case GET_LIST: {
      LOG.debug(String.format("INTO next with GET LIST and Key (%s)", Bytes.toString(nextKey)));
      
      if (versions == 1) {
        if (nextKey != null) {
          LOG.debug(String.format("Processing Key (%s)", Bytes.toString(nextKey)));
          
          Get theGet = new Get(nextKey);
          theGet.setMaxVersions(versions);

          Result result = this.htable.get(theGet);

          if (result != null && (! result.isEmpty()) ) {
            LOG.debug(String.format("Key (%s), Version (%s), Got Result (%s)", Bytes.toString(nextKey), versions, result ) );

            if (keyList != null || !keyList.isEmpty()) {
              String newKey = keyList.pollFirst();
              LOG.debug("New Key => " + newKey);
              nextKey = (newKey == null || newKey.length() == 0) ? null : Bytes
                  .toBytes(newKey);
            } else {
              nextKey = null;
            }
            
            LOG.debug(String.format("=> Picked a new Key (%s)", Bytes.toString(nextKey)));
            
            // Write the result
            if( useSalt) {
              key.set( HBaseSalter.delSaltPrefix(result.getRow()));
            } else {
              key.set(result.getRow());
            }
            lastSuccessfulRow = key.get();
            Writables.copyWritable(result, value);
            
            return true;
          } else {
            LOG.debug(" Key ("+ Bytes.toString(nextKey)+ ") return an EMPTY result. Get ("+theGet.getId()+")" ); //alg0
            
            String newKey;
            while((newKey = keyList.pollFirst()) != null) {
              LOG.debug("WHILE NEXT Key => " + newKey);

              nextKey = (newKey == null || newKey.length() == 0) ? null : Bytes
                  .toBytes(newKey);
              
              if( nextKey == null ) {
                LOG.error("BOMB! BOMB! BOMB!");
                continue; 
              }
              
              if( ! this.htable.exists( new Get(nextKey) ) ) {
                LOG.debug(String.format("Key (%s) Does not exist in Table (%s)", Bytes.toString(nextKey), Bytes.toString(this.htable.getTableName()) ));
                continue; 
              } else { break; }
            }
            
            nextKey = (newKey == null || newKey.length() == 0) ? null : Bytes
                .toBytes(newKey);
            
            LOG.debug("Final New Key => " + Bytes.toString(nextKey));

            return next(key, value);
          }
        } else {
          // Nothig left. return false
          return false;
        }
      } else {
        if (resultVector != null && resultVector.size() != 0) { 
          LOG.debug(String.format("+ Version (%s), Result VECTOR <%s>", versions, resultVector ) );

          List<KeyValue> resultKeyValue = resultVector.remove(resultVector.size() - 1);
          Result result = new Result(resultKeyValue);

          LOG.debug(String.format("+ Version (%s), Got Result <%s>", versions, result ) );

          if( useSalt) {
            key.set( HBaseSalter.delSaltPrefix(result.getRow()));
          } else {
            key.set(result.getRow());
          }
          lastSuccessfulRow = key.get();
          Writables.copyWritable(result, value);

          return true;
        } else {
          if (nextKey != null) {
            LOG.debug(String.format("+ Processing Key (%s)", Bytes.toString(nextKey)));
            
            Get theGet = new Get(nextKey);
            theGet.setMaxVersions(versions);

            Result resultAll = this.htable.get(theGet);
            
            if( resultAll != null && (! resultAll.isEmpty())) {
              List<KeyValue> keyValeList = resultAll.list();

              keyValueMap = new HashMap<Long, List<KeyValue>>();
              
              LOG.debug(String.format("+ Key (%s) Versions (%s) Val;ute map <%s>", Bytes.toString(nextKey), versions, keyValueMap));

              for (KeyValue keyValue : keyValeList) {
                long version = keyValue.getTimestamp();

                if (keyValueMap.containsKey(new Long(version))) {
                  List<KeyValue> keyValueTempList = keyValueMap.get(new Long(
                      version));
                  if (keyValueTempList == null) {
                    keyValueTempList = new ArrayList<KeyValue>();
                  }
                  keyValueTempList.add(keyValue);
                } else {
                  List<KeyValue> keyValueTempList = new ArrayList<KeyValue>();
                  keyValueMap.put(new Long(version), keyValueTempList);
                  keyValueTempList.add(keyValue);
                }
              }

              resultVector = new Vector<List<KeyValue>>();
              resultVector.addAll(keyValueMap.values());

              List<KeyValue> resultKeyValue = resultVector.remove(resultVector.size() - 1);

              Result result = new Result(resultKeyValue);

              LOG.debug(String.format("+ Version (%s), Got Result (%s)", versions, result ) );

              String newKey = keyList.pollFirst(); // Bytes.toString(resultKeyValue.getKey());//

              nextKey = (newKey == null || newKey.length() == 0) ? null : Bytes
                  .toBytes(newKey);

              if( useSalt) {
                key.set( HBaseSalter.delSaltPrefix(result.getRow()));
              } else {
                key.set(result.getRow());
              }
              lastSuccessfulRow = key.get();
              Writables.copyWritable(result, value);
              return true;
            } else {
              LOG.debug(String.format("+ Key (%s) return an EMPTY result. Get (%s)", Bytes.toString(nextKey), theGet.getId()) ); //alg0
              
              String newKey;
              
              while( (newKey = keyList.pollFirst()) != null ) {
                LOG.debug("+ WHILE NEXT Key => " + newKey);

                nextKey = (newKey == null || newKey.length() == 0) ? null : Bytes
                    .toBytes(newKey);
                
                if( nextKey == null ) {
                  LOG.error("+ BOMB! BOMB! BOMB!");
                  continue; 
                }
                
                if( ! this.htable.exists( new Get(nextKey) ) ) {
                  LOG.debug(String.format("+ Key (%s) Does not exist in Table (%s)", Bytes.toString(nextKey), Bytes.toString(this.htable.getTableName()) ));
                  continue; 
                } else { break; }
              }
              
              nextKey = (newKey == null || newKey.length() == 0) ? null : Bytes
                  .toBytes(newKey);
              
              LOG.debug("+ Final New Key => " + Bytes.toString(nextKey));

              return next(key, value);
            }
            
          } else {
            return false;
          }
        }
      }
    }
    default:
      throw new IOException("Unknown source mode : " + sourceMode);
    }
  }
}
