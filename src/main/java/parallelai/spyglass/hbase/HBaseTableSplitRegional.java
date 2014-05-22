package parallelai.spyglass.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTableSplitRegional extends HBaseTableSplitBase {

	private final Log LOG = LogFactory.getLog(HBaseTableSplitRegional.class);

	private List<HBaseTableSplitGranular> splits = new Vector<HBaseTableSplitGranular>();

	/** default constructor */
	private HBaseTableSplitRegional() {

	}

	public HBaseTableSplitRegional(String regionLocation, String regionName) {
		this.m_regionLocation = regionLocation;
        this.m_regionName = regionName;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		LOG.debug("REGIONAL READ ME : " + in.toString());

        super.readFields(in);

		int s = Bytes.toInt(Bytes.readByteArray(in));

		for (int i = 0; i < s; i++) {
			HBaseTableSplitGranular hbts = new HBaseTableSplitGranular();
            hbts.readFields(in);

			splits.add(hbts);
		}

		LOG.debug("REGIONAL READ and CREATED : " + this);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		LOG.debug("REGIONAL WRITE : " + this);

        super.write(out);

		Bytes.writeByteArray(out, Bytes.toBytes(splits.size()));

		for (HBaseTableSplitGranular hbts : splits) {
			hbts.write(out);
		}

		LOG.debug("REGIONAL WROTE : " + out.toString());
	}

	@Override
	public String toString() {
		StringBuffer str = new StringBuffer();
		str.append("HBaseTableSplitRegional : ");

        str.append(super.toString());

        str.append(" REGIONAL => Region Location (" + m_regionLocation + ") Name (" + m_regionName + ")" );

        str.append(" GRANULAR = > ");

		for (HBaseTableSplitGranular hbt : splits) {
			str.append(" [" + hbt.toString() + "]");
		}

		return str.toString();
	}

	@Override
	public int compareTo(HBaseTableSplitBase o) {
        if( ! (o instanceof HBaseTableSplitRegional) ) return -1;

		return (splits.size() - ((HBaseTableSplitRegional)o).splits.size());
	}

	@Override
	public long getLength() throws IOException {
		return splits.size();
	}

	public void addSplit(HBaseTableSplitGranular hbt) throws IOException {
        LOG.debug("ADD Split : " + hbt);

		if (hbt.getRegionLocation().equals(m_regionLocation)) {
			splits.add(hbt);
            this.copy(hbt);
        } else
			throw new IOException("HBaseTableSplitGranular Region Location "
					+ hbt.getRegionLocation()
					+ " does NOT match MultiSplit Region Location " + m_regionLocation);
	}

//	public List<HBaseTableSplitGranular> getSplits() {
//		return splits;
//	}

    public boolean hasMoreSplits() {
        splitIterator = (splitIterator == null) ? splits.listIterator() : splitIterator;

        return splitIterator.hasNext();
    }

    private Iterator<HBaseTableSplitGranular> splitIterator = null;
    private int currSplitCount = 0;

    public HBaseTableSplitGranular getNextSplit() {
        splitIterator = (splitIterator == null) ? splits.listIterator() : splitIterator;

        if( splitIterator.hasNext() ) {
            currSplitCount ++;
            return splitIterator.next();
        } else {
            return null;
        }
    }

    public int getCurrSplitCount() {
        return currSplitCount;
    }
}