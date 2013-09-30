package parallelai.spyglass.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ScannerCallable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.RecordReader;

import java.util.TreeSet;

import static org.apache.hadoop.hbase.mapreduce.TableRecordReaderImpl.LOG_PER_ROW_COUNT;

/**
 * Created with IntelliJ IDEA.
 * User: chand_000
 * Date: 29/08/13
 * Time: 15:42
 * To change this template use File | Settings | File Templates.
 */
public abstract class HBaseRecordReaderBase implements
        RecordReader<ImmutableBytesWritable, Result> {

    protected TreeSet<String> keyList;
    protected long initialNoOfKeys = 0;
    protected HBaseConstants.SourceMode sourceMode;
    protected boolean endRowInclusive = true;
    protected int versions = 1;
    protected boolean useSalt = false;

    protected byte[] startRow;
    protected byte[] endRow;

    protected HTable htable;
    protected byte[][] trrInputColumns;

    protected Filter trrRowFilter;

    protected boolean logScannerActivity = false;
    protected int logPerRowCount = 100;
    protected int noOfLogCount = 0;

    @Override
    public String toString() {
        StringBuffer sbuf = new StringBuffer();

        sbuf.append("".format("HBaseRecordReaderRegional : startRow [%s] endRow [%s] endRowInc [%s] ",
                Bytes.toString(startRow), Bytes.toString(endRow), endRowInclusive));
        sbuf.append("".format(" sourceMode [%s] salt [%s] versions [%s] ",
                sourceMode, useSalt, versions));

        return sbuf.toString();
    }

    byte[] getStartRow() {
        return this.startRow;
    }

    /**
     * @param htable
     *          the {@link org.apache.hadoop.hbase.client.HTable} to scan.
     */
    public void setHTable(HTable htable) {
        Configuration conf = htable.getConfiguration();
        logScannerActivity = conf.getBoolean(ScannerCallable.LOG_SCANNER_ACTIVITY,
                false);
        logPerRowCount = conf.getInt(LOG_PER_ROW_COUNT, 100);
        this.htable = htable;
    }

    /**
     * @param inputColumns
     *          the columns to be placed in {@link Result}.
     */
    public void setInputColumns(final byte[][] inputColumns) {
        this.trrInputColumns = inputColumns;
    }

    /**
     * @param startRow
     *          the first row in the split
     */
    public void setStartRow(final byte[] startRow) {
        this.startRow = startRow;
    }

    /**
     *
     * @param endRow
     *          the last row in the split
     */
    public void setEndRow(final byte[] endRow) {
        this.endRow = endRow;
    }

    /**
     * @param rowFilter
     *          the {@link org.apache.hadoop.hbase.filter.Filter} to be used.
     */
    public void setRowFilter(Filter rowFilter) {
        this.trrRowFilter = rowFilter;
    }

    public TreeSet<String> getKeyList() {
        return keyList;
    }

    public void setKeyList(TreeSet<String> keyList) {
        this.keyList = keyList;
        initialNoOfKeys = (this.keyList == null) ? 0 : this.keyList.size();
    }

    public void setVersions(int versions) {
        this.versions = versions;
    }

    public void setUseSalt(boolean useSalt) {
        this.useSalt = useSalt;
    }

    public HBaseConstants.SourceMode getSourceMode() {
        return sourceMode;
    }

    public void setSourceMode(HBaseConstants.SourceMode sourceMode) {
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

}
