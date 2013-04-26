package parallelai.spyglass.hbase;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.StringUtils;

import parallelai.spyglass.hbase.HBaseConstants.SourceMode;


public class HBaseInputFormat
  implements InputFormat<ImmutableBytesWritable, Result>, JobConfigurable {
  
  private final Log LOG = LogFactory.getLog(HBaseInputFormat.class);

  private final String id = UUID.randomUUID().toString();

  private byte [][] inputColumns;
  private HTable table;
  private HBaseRecordReader tableRecordReader;
  private Filter rowFilter;
  private String tableName = "";

  private HashMap<InetAddress, String> reverseDNSCacheMap =
    new HashMap<InetAddress, String>();
  
  private String nameServer = null;
  
//  private Scan scan = null;

  
  @SuppressWarnings("deprecation")
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    if (this.table == null) {
      throw new IOException("No table was provided");
    }
    
    if (this.inputColumns == null || this.inputColumns.length == 0) {
      throw new IOException("Expecting at least one column");
    }
    
    Pair<byte[][], byte[][]> keys = table.getStartEndKeys();

    if (keys == null || keys.getFirst() == null || keys.getFirst().length == 0) {
      HRegionLocation regLoc = table.getRegionLocation(HConstants.EMPTY_BYTE_ARRAY, false);

      if (null == regLoc) {
        throw new IOException("Expecting at least one region.");
      }

      List<HBaseTableSplit> splits = new ArrayList<HBaseTableSplit>(1);
      HBaseTableSplit split = new HBaseTableSplit(table.getTableName(),
          HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, regLoc
              .getHostnamePort().split(Addressing.HOSTNAME_PORT_SEPARATOR)[0], SourceMode.EMPTY);
      splits.add(split);
      
      return splits.toArray(new HBaseTableSplit[splits.size()]);
    }
    
    if( keys.getSecond() == null || keys.getSecond().length == 0) {
      throw new IOException("Expecting at least one region.");
    }
    
    if( keys.getFirst().length != keys.getSecond().length ) {
      throw new IOException("Regions for start and end key do not match");
    }
    
    byte[] minKey = keys.getFirst()[keys.getFirst().length - 1];
    byte[] maxKey = keys.getSecond()[0];
    
    LOG.info( "".format("SETTING min key (%s) and max key (%s)", Bytes.toString(minKey), Bytes.toString(maxKey)));

    byte [][] regStartKeys = keys.getFirst();
    byte [][] regStopKeys = keys.getSecond();
    String [] regions = new String[regStartKeys.length];
    
    for( int i = 0; i < regStartKeys.length; i++ ) {
      minKey = (regStartKeys[i] != null && regStartKeys[i].length != 0 ) && (Bytes.compareTo(regStartKeys[i], minKey) < 0 ) ? regStartKeys[i] : minKey;
      maxKey = (regStopKeys[i] != null && regStopKeys[i].length != 0) && (Bytes.compareTo(regStopKeys[i], maxKey) > 0 ) ? regStopKeys[i] : maxKey;
      
      HServerAddress regionServerAddress = 
          table.getRegionLocation(keys.getFirst()[i]).getServerAddress();
        InetAddress regionAddress =
          regionServerAddress.getInetSocketAddress().getAddress();
        String regionLocation;
        try {
          regionLocation = reverseDNS(regionAddress);
        } catch (NamingException e) {
          LOG.error("Cannot resolve the host name for " + regionAddress +
              " because of " + e);
          regionLocation = regionServerAddress.getHostname();
        }

//       HServerAddress regionServerAddress = table.getRegionLocation(keys.getFirst()[i]).getServerAddress();
//      InetAddress regionAddress = regionServerAddress.getInetSocketAddress().getAddress();
//
//      String regionLocation;
//
//      try {
//        regionLocation = reverseDNS(regionAddress);
//      } catch (NamingException e) {
//        LOG.error("Cannot resolve the host name for " + regionAddress + " because of " + e);
//        regionLocation = regionServerAddress.getHostname();
//      }

//      String regionLocation = table.getRegionLocation(keys.getFirst()[i]).getHostname();
      
      LOG.debug( "***** " + regionLocation );
      
      if( regionLocation == null || regionLocation.length() == 0 )
        throw new IOException( "The region info for regiosn " + i + " is null or empty");

      regions[i] = regionLocation;
      
      LOG.info("".format("Region (%s) has start key (%s) and stop key (%s)", regions[i], Bytes.toString(regStartKeys[i]), Bytes.toString(regStopKeys[i]) ));
    }
    
    byte[] startRow = HConstants.EMPTY_START_ROW;
    byte[] stopRow = HConstants.EMPTY_END_ROW;
    
    LOG.info( "".format("Found min key (%s) and max key (%s)", Bytes.toString(minKey), Bytes.toString(maxKey)));
    
    LOG.info("SOURCE MODE is : " + sourceMode);    

    switch( sourceMode ) {
    case SCAN_ALL:
      startRow = HConstants.EMPTY_START_ROW;
      stopRow = HConstants.EMPTY_END_ROW;

      LOG.info( "".format("SCAN ALL: Found start key (%s) and stop key (%s)", Bytes.toString(startRow), Bytes.toString(stopRow)));
      break;
      
    case SCAN_RANGE:
      startRow = (startKey != null && startKey.length() != 0) ? Bytes.toBytes(startKey) : HConstants.EMPTY_START_ROW ;
      stopRow = (stopKey != null && stopKey.length() != 0) ? Bytes.toBytes(stopKey) : HConstants.EMPTY_END_ROW ;

      LOG.info( "".format("SCAN RANGE: Found start key (%s) and stop key (%s)", Bytes.toString(startRow), Bytes.toString(stopRow)));
      break;
    }
    
    switch( sourceMode ) {
      case EMPTY:
      case SCAN_ALL:
      case SCAN_RANGE:
      {
//        startRow = (Bytes.compareTo(startRow, minKey) < 0) ? minKey : startRow;
//        stopRow = (Bytes.compareTo(stopRow, maxKey) > 0) ? maxKey : stopRow;
        
        List<HBaseTableSplit> splits = new ArrayList<HBaseTableSplit>();
        
        List<HRegionLocation> validRegions = table.getRegionsInRange(startRow, stopRow);
        
        int maxRegions = validRegions.size();
        int currentRegion = 1;
        
        for( HRegionLocation cRegion : validRegions ) {
          byte [] rStart = cRegion.getRegionInfo().getStartKey();
          byte [] rStop = cRegion.getRegionInfo().getEndKey();
          
          HServerAddress regionServerAddress = cRegion.getServerAddress();
            InetAddress regionAddress =
              regionServerAddress.getInetSocketAddress().getAddress();
            String regionLocation;
            try {
              regionLocation = reverseDNS(regionAddress);
            } catch (NamingException e) {
              LOG.error("Cannot resolve the host name for " + regionAddress +
                  " because of " + e);
              regionLocation = regionServerAddress.getHostname();
            }
            
            byte [] sStart = (startRow == HConstants.EMPTY_START_ROW || (Bytes.compareTo(startRow, rStart) <= 0 ) ? rStart : startRow);
            byte [] sStop = (stopRow == HConstants.EMPTY_END_ROW || (Bytes.compareTo(stopRow, rStop) >= 0 && rStop.length != 0) ? rStop : stopRow); 
            
            LOG.info("".format("BOOL start (%s) stop (%s) length (%d)", 
                (startRow == HConstants.EMPTY_START_ROW || (Bytes.compareTo(startRow, rStart) <= 0 )),
                    (stopRow == HConstants.EMPTY_END_ROW || (Bytes.compareTo(stopRow, rStop) >= 0 )),
                    rStop.length
                    ));
          
          HBaseTableSplit split = new HBaseTableSplit(
              table.getTableName(),
              sStart,
              sStop,
              regionLocation,
              SourceMode.SCAN_RANGE
           );
          
          split.setEndRowInclusive( currentRegion == maxRegions );

          currentRegion ++;
                  
           LOG.info("".format("START KEY (%s) STOP KEY (%s) rSTART (%s) rSTOP (%s) sSTART (%s) sSTOP (%s) REGION [%s] SPLIT [%s]", 
               Bytes.toString(startRow), Bytes.toString(stopRow),
               Bytes.toString(rStart), Bytes.toString(rStop),
               Bytes.toString(sStart), 
               Bytes.toString(sStop), 
               cRegion.getHostnamePort(), split) );
          
           splits.add(split);
        }
        
//
//        for (int i = 0; i < keys.getFirst().length; i++) {
//
//          if ( ! includeRegionInSplit(keys.getFirst()[i], keys.getSecond()[i])) {
//            LOG.info("NOT including regions : " + regions[i]);
//            continue;
//          }
//          
//          // determine if the given start an stop key fall into the region
//          if ((startRow.length == 0 || keys.getSecond()[i].length == 0 ||
//               Bytes.compareTo(startRow, keys.getSecond()[i]) < 0) &&
//              (stopRow.length == 0 ||
//               Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {
//            
//            byte[] splitStart = startRow.length == 0 ||
//              Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ?
//                keys.getFirst()[i] : startRow;
//            byte[] splitStop = (stopRow.length == 0 ||
//              Bytes.compareTo(keys.getSecond()[i], stopRow) <= 0) &&
//              keys.getSecond()[i].length > 0 ?
//                keys.getSecond()[i] : stopRow;
//                HBaseTableSplit split = new HBaseTableSplit(table.getTableName(),
//              splitStart, splitStop, regions[i], SourceMode.SCAN_RANGE);
//            splits.add(split);
//            
//            LOG.info("getSplits: split -> " + i + " -> " + split);
//          }
//        }
        
        LOG.info("RETURNED SPLITS: split -> " + splits);

        return splits.toArray(new HBaseTableSplit[splits.size()]);
      } 
        
      case GET_LIST:
      {
        if( keyList == null || keyList.size() == 0 ) {
          throw new IOException("Source Mode is GET_LIST but key list is EMPTY");
        } 
        
        List<HBaseTableSplit> splits = new ArrayList<HBaseTableSplit>();
        
        for (int i = 0; i < keys.getFirst().length; i++) {

          if ( ! includeRegionInSplit(keys.getFirst()[i], keys.getSecond()[i])) {
            continue;
          }
          
          LOG.info("".format("Getting region (%s) subset (%s) to (%s)", regions[i], Bytes.toString(regStartKeys[i]), Bytes.toString(regStartKeys[i] )));

          Set<String> regionsSubSet = null;
          
          if( (regStartKeys[i] == null || regStartKeys[i].length == 0) && (regStopKeys[i] == null || regStopKeys[i].length == 0) ) {
            LOG.info("REGION start is empty");
            LOG.info("REGION stop is empty");
            regionsSubSet = keyList;
          } else if( regStartKeys[i] == null || regStartKeys[i].length == 0 ) {
            LOG.info("REGION start is empty");
            regionsSubSet = keyList.headSet(Bytes.toString(regStopKeys[i]), true);
          } else if( regStopKeys[i] == null || regStopKeys[i].length == 0 ) {
            LOG.info("REGION stop is empty");
            regionsSubSet = keyList.tailSet(Bytes.toString(regStartKeys[i]), true);
          } else if( Bytes.compareTo(regStartKeys[i], regStopKeys[i]) <= 0 ) {
            regionsSubSet = keyList.subSet(Bytes.toString(regStartKeys[i]), true, Bytes.toString(regStopKeys[i]), true);
          } else {
            throw new IOException("".format("For REGION (%s) Start Key (%s) > Stop Key(%s)", 
                regions[i], Bytes.toString(regStartKeys[i]), Bytes.toString(regStopKeys[i])));
          }
          
          if( regionsSubSet == null || regionsSubSet.size() == 0) {
            LOG.info( "EMPTY: Key is for region " + regions[i] + " is null");
            
            continue;
          }

          TreeSet<String> regionKeyList = new TreeSet<String>(regionsSubSet);

          LOG.info("".format("Regions [%s] has key list <%s>", regions[i], regionKeyList ));
            
          HBaseTableSplit split = new HBaseTableSplit(
              table.getTableName(), regionKeyList,
              regions[i], 
              SourceMode.GET_LIST);
          splits.add(split);
        }
          
        return splits.toArray(new HBaseTableSplit[splits.size()]);
      } 

      default:
        throw new IOException("Unknown source Mode : " + sourceMode );
    }
  }
  
  private String reverseDNS(InetAddress ipAddress) throws NamingException {
    String hostName = this.reverseDNSCacheMap.get(ipAddress);
    if (hostName == null) {
      hostName = Strings.domainNamePointerToHostName(DNS.reverseDns(ipAddress, this.nameServer));
      this.reverseDNSCacheMap.put(ipAddress, hostName);
    }
    return hostName;
  }


  @Override
  public RecordReader<ImmutableBytesWritable, Result> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    
    if( ! (split instanceof HBaseTableSplit ) )
      throw new IOException("Table Split is not type HBaseTableSplit");

    HBaseTableSplit tSplit = (HBaseTableSplit) split;
    
    HBaseRecordReader trr = new HBaseRecordReader();

    switch( tSplit.getSourceMode() ) {
      case SCAN_ALL:
      case SCAN_RANGE:
      {
        LOG.info("".format("For split [%s] we have start key (%s) and stop key (%s)", tSplit, tSplit.getStartRow(), tSplit.getEndRow() ));
        
        trr.setStartRow(tSplit.getStartRow());
        trr.setEndRow(tSplit.getEndRow());
        trr.setEndRowInclusive(tSplit.getEndRowInclusive());
      }
      
      break;
      
      case GET_LIST:
      {
        LOG.info("".format("For split [%s] we have key list (%s)", tSplit, tSplit.getKeyList() ));
        
        trr.setKeyList(tSplit.getKeyList());
      }
      
      break;
      
      default:
        throw new IOException( "Unknown source mode : " + tSplit.getSourceMode() );
    }
    
    trr.setSourceMode(tSplit.getSourceMode());
    trr.setHTable(this.table);
    trr.setInputColumns(this.inputColumns);
    trr.setRowFilter(this.rowFilter);

    trr.init();

    return trr;
  }
  
  
  
  /* Configuration Section */

  /**
   * space delimited list of columns
   */
  public static final String COLUMN_LIST = "hbase.tablecolumns";

  /**
   * Use this jobconf param to specify the input table
   */
  private static final String INPUT_TABLE = "hbase.inputtable";

  private String startKey = null;
  private String stopKey = null;
  
  private SourceMode sourceMode = SourceMode.EMPTY;
  private TreeSet<String> keyList = null;
  
  public void configure(JobConf job) {
    String tableName = getTableName(job);
    String colArg = job.get(COLUMN_LIST);
    String[] colNames = colArg.split(" ");
    byte [][] m_cols = new byte[colNames.length][];
    for (int i = 0; i < m_cols.length; i++) {
      m_cols[i] = Bytes.toBytes(colNames[i]);
    }
    setInputColumns(m_cols);
    
    try {
      setHTable(new HTable(HBaseConfiguration.create(job), tableName));
    } catch (Exception e) {
      LOG.error( "************* Table could not be created" );
      LOG.error(StringUtils.stringifyException(e));
    }
    
    LOG.debug("Entered : " + this.getClass() + " : configure()" );
    
    sourceMode = SourceMode.valueOf( job.get( String.format(HBaseConstants.SOURCE_MODE, getTableName(job) ) ) ) ;
    
    LOG.info( "".format("GOT SOURCE MODE (%s) as (%s) and finally", 
        String.format(HBaseConstants.SOURCE_MODE, getTableName(job) ), job.get( String.format(HBaseConstants.SOURCE_MODE, getTableName(job) )), sourceMode ));

    switch( sourceMode ) {
      case SCAN_RANGE:
        LOG.info("HIT SCAN_RANGE");
        
        startKey = getJobProp(job, String.format(HBaseConstants.START_KEY, getTableName(job) ) );
        stopKey = getJobProp(job, String.format(HBaseConstants.STOP_KEY, getTableName(job) ) );

        LOG.info(String.format("Setting start key (%s) and stop key (%s)", startKey, stopKey) );
        break;
        
      case GET_LIST:
        LOG.info("HIT GET_LIST");
        
        Collection<String> keys = job.getStringCollection(String.format(HBaseConstants.KEY_LIST, getTableName(job)));
        keyList = new TreeSet<String> (keys);
        
        LOG.info( "GOT KEY LIST : " + keys );
        LOG.info(String.format("SETTING key list (%s)", keyList) );

        break;
        
      case EMPTY:
        LOG.info("HIT EMPTY");
        
        sourceMode = sourceMode.SCAN_ALL;
        break;
      
      default:
        LOG.info("HIT DEFAULT");
        
        break;
    }
  }

  public void validateInput(JobConf job) throws IOException {
    // expecting exactly one path
    String tableName = getTableName(job);
    
    if (tableName == null) {
      throw new IOException("expecting one table name");
    }
    LOG.debug("".format("Found Table name [%s]", tableName));
    

    // connected to table?
    if (getHTable() == null) {
      throw new IOException("could not connect to table '" +
        tableName + "'");
    }
    LOG.debug("".format("Found Table [%s]", getHTable().getTableName()));

    // expecting at least one column
    String colArg = job.get(COLUMN_LIST);
    if (colArg == null || colArg.length() == 0) {
      throw new IOException("expecting at least one column");
    }
    LOG.debug("".format("Found Columns [%s]", colArg));

    LOG.debug("".format("Found Start & STop Key [%s][%s]", startKey, stopKey));
    
    if( sourceMode == SourceMode.EMPTY ) {
      throw new IOException("SourceMode should not be EMPTY");
    }
    
    if( sourceMode == SourceMode.GET_LIST && (keyList == null || keyList.size() == 0) ) {
      throw new IOException( "Source mode is GET_LIST bu key list is empty");
    }
  }
  
  
  /* Getters & Setters */
  private HTable getHTable() { return this.table; }
  private void setHTable(HTable ht) { this.table = ht; }
  private void setInputColumns( byte [][] ic ) { this.inputColumns = ic; }

  
  private void setJobProp( JobConf job, String key, String value) {
    if( job.get(key) != null ) throw new RuntimeException("".format("Job Conf already has key [%s] with value [%s]", key, job.get(key)));
    job.set(key,  value);
  }
  
  private String getJobProp( JobConf job, String key ) { return job.get(key); }
  
  public static void setTableName(JobConf job, String tableName) {
    // Make sure that table has not been set before
    String oldTableName = getTableName(job);
    if(oldTableName != null) {
      throw new RuntimeException("table name already set to: '"
        + oldTableName + "'");
    }
    
    job.set(INPUT_TABLE, tableName);
  }
  
  public static String getTableName(JobConf job) {
    return job.get(INPUT_TABLE);
  }

  protected boolean includeRegionInSplit(final byte[] startKey, final byte [] endKey) {
    return true;
  }
}
