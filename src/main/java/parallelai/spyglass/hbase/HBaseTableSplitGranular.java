package parallelai.spyglass.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.InputSplit;

import parallelai.spyglass.hbase.HBaseConstants.SourceMode;

public class HBaseTableSplitGranular extends HBaseTableSplitBase {

	private final Log LOG = LogFactory.getLog(HBaseTableSplitGranular.class);

    /** default constructor */
    public HBaseTableSplitGranular() {
        this(HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY,
                HConstants.EMPTY_BYTE_ARRAY, "", "", HBaseConstants.SourceMode.EMPTY, false);
    }

    /**
     * Constructor
     *
     * @param tableName
     * @param startRow
     * @param endRow
     * @param location
     */
    public HBaseTableSplitGranular(final byte[] tableName, final byte[] startRow,
                               final byte[] endRow, final String location, final String regionName,
                               final HBaseConstants.SourceMode sourceMode, final boolean useSalt) {
        this.m_tableName = tableName;
        this.m_startRow = startRow;
        this.m_endRow = endRow;
        this.m_regionLocation = location;
        this.m_regionName = regionName;
        this.m_sourceMode = sourceMode;
        this.m_useSalt = useSalt;
    }

    public HBaseTableSplitGranular(final byte[] tableName,
                               final TreeSet<String> keyList, int versions, final String location, final String regionName,
                               final HBaseConstants.SourceMode sourceMode, final boolean useSalt) {
        this.m_tableName = tableName;
        this.m_keyList = keyList;
        this.m_versions = versions;
        this.m_sourceMode = sourceMode;
        this.m_regionLocation = location;
        this.m_regionName = regionName;
        this.m_useSalt = useSalt;
    }


	@Override
	public long getLength() {
		// Not clear how to obtain this... seems to be used only for sorting
		// splits
		return 0;
	}


	@Override
	public String toString() {
		return String
				.format(
						"Table Name (%s) Region Location (%s) Name (%s) Source Mode (%s) Start Key (%s) Stop Key (%s) Key List Size (%s) Versions (%s) Use Salt (%s)",
						Bytes.toString(m_tableName), m_regionLocation, m_regionName, m_sourceMode,
						Bytes.toString(m_startRow), Bytes.toString(m_endRow),
						(m_keyList != null) ? m_keyList.size() : "EMPTY", m_versions,
						m_useSalt);
	}

	@Override
	public int compareTo(HBaseTableSplitBase o) {
        if( ! (o instanceof HBaseTableSplitGranular) ) return -1;

		switch (m_sourceMode) {
			case SCAN_ALL:
			case SCAN_RANGE:
				return Bytes.compareTo(getStartRow(), o.getStartRow());

			case GET_LIST:
				return m_keyList.equals(o.getKeyList()) ? 0 : -1;

			case EMPTY:
				return 0;

			default:
				return -1;
		}

	}
}