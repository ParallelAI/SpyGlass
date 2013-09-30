package parallelai.spyglass.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.TreeSet;

/**
 * Created with IntelliJ IDEA.
 * User: chand_000
 * Date: 29/08/13
 * Time: 16:18
 * To change this template use File | Settings | File Templates.
 */
public abstract class HBaseTableSplitBase implements InputSplit,
        Comparable<HBaseTableSplitBase>, Serializable {

    private final Log LOG = LogFactory.getLog(HBaseTableSplitBase.class);


    protected byte[] m_tableName = null;
    protected byte[] m_startRow = null;
    protected byte[] m_endRow = null;
    protected String m_regionLocation = null;
    protected String m_regionName = null;
    protected TreeSet<String> m_keyList = null;
    protected HBaseConstants.SourceMode m_sourceMode = HBaseConstants.SourceMode.EMPTY;
    protected boolean m_endRowInclusive = true;
    protected int m_versions = 1;
    protected boolean m_useSalt = false;


    /** @return table name */
    public byte[] getTableName() {
        return this.m_tableName;
    }

    /** @return starting row key */
    public byte[] getStartRow() {
        return this.m_startRow;
    }

    /** @return end row key */
    public byte[] getEndRow() {
        return this.m_endRow;
    }

    public boolean getEndRowInclusive() {
        return m_endRowInclusive;
    }

    public void setEndRowInclusive(boolean isInclusive) {
        m_endRowInclusive = isInclusive;
    }

    /** @return list of keys to get */
    public TreeSet<String> getKeyList() {
        return m_keyList;
    }

    public int getVersions() {
        return m_versions;
    }

    /** @return get the source mode */
    public HBaseConstants.SourceMode getSourceMode() {
        return m_sourceMode;
    }

    public boolean getUseSalt() {
        return m_useSalt;
    }

    /** @return the region's hostname */
    public String getRegionLocation() {
        LOG.debug("REGION GETTER : " + m_regionLocation);

        return this.m_regionLocation;
    }

    public String[] getLocations() {
        LOG.debug("REGION ARRAY : " + m_regionLocation);

        return new String[] { this.m_regionLocation };
    }

    public String getRegionName() {
        return this.m_regionName;
    }


    public void copy(HBaseTableSplitBase that) {
        this.m_endRow = that.m_endRow;
        this.m_endRowInclusive = that.m_endRowInclusive;
        this.m_keyList = that.m_keyList;
        this.m_sourceMode = that.m_sourceMode;
        this.m_startRow = that.m_startRow;
        this.m_tableName = that.m_tableName;
        this.m_useSalt = that.m_useSalt;
        this.m_versions = that.m_versions;
        this.m_regionLocation = that.m_regionLocation;
        this.m_regionName = that.m_regionName;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        LOG.debug("READ ME : " + in.toString());

        this.m_tableName = Bytes.readByteArray(in);
        this.m_regionLocation = Bytes.toString(Bytes.readByteArray(in));
        this.m_regionName = Bytes.toString(Bytes.readByteArray(in));
        this.m_sourceMode = HBaseConstants.SourceMode.valueOf(Bytes.toString(Bytes
                .readByteArray(in)));
        this.m_useSalt = Bytes.toBoolean(Bytes.readByteArray(in));

        switch (this.m_sourceMode) {
            case SCAN_RANGE:
                this.m_startRow = Bytes.readByteArray(in);
                this.m_endRow = Bytes.readByteArray(in);
                this.m_endRowInclusive = Bytes.toBoolean(Bytes.readByteArray(in));
                break;

            case GET_LIST:
                this.m_versions = Bytes.toInt(Bytes.readByteArray(in));
                this.m_keyList = new TreeSet<String>();

                int m = Bytes.toInt(Bytes.readByteArray(in));

                for (int i = 0; i < m; i++) {
                    this.m_keyList.add(Bytes.toString(Bytes.readByteArray(in)));
                }
                break;
        }

        LOG.debug("READ and CREATED : " + this);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        LOG.debug("WRITE : " + this);

        Bytes.writeByteArray(out, this.m_tableName);
        Bytes.writeByteArray(out, Bytes.toBytes(this.m_regionLocation));
        Bytes.writeByteArray(out, Bytes.toBytes(this.m_regionName));
        Bytes.writeByteArray(out, Bytes.toBytes(this.m_sourceMode.name()));
        Bytes.writeByteArray(out, Bytes.toBytes(this.m_useSalt));

        switch (this.m_sourceMode) {
            case SCAN_RANGE:
                Bytes.writeByteArray(out, this.m_startRow);
                Bytes.writeByteArray(out, this.m_endRow);
                Bytes.writeByteArray(out, Bytes.toBytes(this.m_endRowInclusive));
                break;

            case GET_LIST:
                Bytes.writeByteArray(out, Bytes.toBytes(m_versions));
                Bytes.writeByteArray(out, Bytes.toBytes(this.m_keyList.size()));

                for (String k : this.m_keyList) {
                    Bytes.writeByteArray(out, Bytes.toBytes(k));
                }
                break;
        }

        LOG.debug("WROTE : " + out.toString());
    }
}
