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

public class HBaseTableSplit implements InputSplit,
		Comparable<HBaseTableSplit>, Serializable {

	private final Log LOG = LogFactory.getLog(HBaseTableSplit.class);

	private byte[] m_tableName = null;
	private byte[] m_startRow = null;
	private byte[] m_endRow = null;
	private String m_regionLocation = null;
	private TreeSet<String> m_keyList = null;
	private SourceMode m_sourceMode = SourceMode.EMPTY;
	private boolean m_endRowInclusive = true;
	private int m_versions = 1;
	private boolean m_useSalt = false;

	/** default constructor */
	public HBaseTableSplit() {
		this(HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY,
				HConstants.EMPTY_BYTE_ARRAY, "", SourceMode.EMPTY, false);
	}

	/**
	 * Constructor
	 * 
	 * @param tableName
	 * @param startRow
	 * @param endRow
	 * @param location
	 */
	public HBaseTableSplit(final byte[] tableName, final byte[] startRow,
			final byte[] endRow, final String location,
			final SourceMode sourceMode, final boolean useSalt) {
		this.m_tableName = tableName;
		this.m_startRow = startRow;
		this.m_endRow = endRow;
		this.m_regionLocation = location;
		this.m_sourceMode = sourceMode;
		this.m_useSalt = useSalt;
	}

	public HBaseTableSplit(final byte[] tableName,
			final TreeSet<String> keyList, int versions, final String location,
			final SourceMode sourceMode, final boolean useSalt) {
		this.m_tableName = tableName;
		this.m_keyList = keyList;
		this.m_versions = versions;
		this.m_sourceMode = sourceMode;
		this.m_regionLocation = location;
		this.m_useSalt = useSalt;
	}

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
	public SourceMode getSourceMode() {
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

	@Override
	public long getLength() {
		// Not clear how to obtain this... seems to be used only for sorting
		// splits
		return 0;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		LOG.debug("READ ME : " + in.toString());

		this.m_tableName = Bytes.readByteArray(in);
		this.m_regionLocation = Bytes.toString(Bytes.readByteArray(in));
		this.m_sourceMode = SourceMode.valueOf(Bytes.toString(Bytes
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

	@Override
	public String toString() {
		return String
				.format(
						"Table Name (%s) Region (%s) Source Mode (%s) Start Key (%s) Stop Key (%s) Key List Size (%s) Versions (%s) Use Salt (%s)",
						Bytes.toString(m_tableName), m_regionLocation, m_sourceMode,
						Bytes.toString(m_startRow), Bytes.toString(m_endRow),
						(m_keyList != null) ? m_keyList.size() : "EMPTY", m_versions,
						m_useSalt);
	}

	@Override
	public int compareTo(HBaseTableSplit o) {
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