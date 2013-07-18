package parallelai.spyglass.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.InputSplit;

public class HBaseMultiInputSplit implements InputSplit,
		Comparable<HBaseMultiInputSplit>, Serializable {

	private final Log LOG = LogFactory.getLog(HBaseMultiInputSplit.class);

	private List<HBaseTableSplit> splits = new ArrayList<HBaseTableSplit>();

	private String regionLocation = null;

	/** default constructor */
	private HBaseMultiInputSplit() {

	}

	public HBaseMultiInputSplit(String regionLocation) {
		this.regionLocation = regionLocation;
	}

	/** @return the region's hostname */
	public String getRegionLocation() {
		LOG.debug("REGION GETTER : " + regionLocation);

		return this.regionLocation;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		LOG.debug("READ ME : " + in.toString());

		int s = Bytes.toInt(Bytes.readByteArray(in));

		for (int i = 0; i < s; i++) {
			HBaseTableSplit hbts = (HBaseTableSplit) SerializationUtils
					.deserialize(Bytes.readByteArray(in));
			splits.add(hbts);
		}

		LOG.debug("READ and CREATED : " + this);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		LOG.debug("WRITE : " + this);

		Bytes.writeByteArray(out, Bytes.toBytes(splits.size()));

		for (HBaseTableSplit hbts : splits) {
			Bytes.writeByteArray(out, SerializationUtils.serialize(hbts));
		}

		LOG.debug("WROTE : " + out.toString());
	}

	@Override
	public String toString() {
		StringBuffer str = new StringBuffer();
		str.append("HBaseMultiSplit : ");

		for (HBaseTableSplit hbt : splits) {
			str.append(" [" + hbt.toString() + "]");
		}

		return str.toString();
	}

	@Override
	public int compareTo(HBaseMultiInputSplit o) {
		// TODO: Make this comparison better
		return (splits.size() - o.splits.size());
	}

	@Override
	public long getLength() throws IOException {
		return splits.size();
	}

	@Override
	public String[] getLocations() throws IOException {
		LOG.debug("REGION ARRAY : " + regionLocation);

		return new String[] { this.regionLocation };
	}

	public void addSplit(HBaseTableSplit hbt) throws IOException {
		if (hbt.getRegionLocation().equals(regionLocation))
			splits.add(hbt);
		else
			throw new IOException("HBaseTableSplit Region Location "
					+ hbt.getRegionLocation()
					+ " does NOT match MultiSplit Region Location " + regionLocation);
	}

	public List<HBaseTableSplit> getSplits() {
		return splits;
	}
}