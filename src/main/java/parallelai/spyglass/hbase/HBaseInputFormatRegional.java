package parallelai.spyglass.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: chand_000
 * Date: 29/08/13
 * Time: 12:24
 * To change this template use File | Settings | File Templates.
 */
public class HBaseInputFormatRegional extends HBaseInputFormatBase {
    private HBaseInputFormatGranular granular = new HBaseInputFormatGranular();
    private final Log LOG = LogFactory.getLog(HBaseInputFormatRegional.class);


    @Override
    public HBaseTableSplitRegional[] getSplits(JobConf job, int numSplits) throws IOException {
        granular.configure(job);
        HBaseTableSplitGranular[] gSplits = granular.getSplits(job, numSplits);

        HBaseTableSplitRegional[] splits = convertToRegionalSplitArray(gSplits);

        if( splits == null ) throw new IOException("Not sure WTF is going on? splits is NULL");

        for(HBaseTableSplitGranular g : gSplits) {
            LOG.info("GRANULAR => " + g);
        }

        for(HBaseTableSplitRegional r : splits ) {
            LOG.info("REGIONAL => " + r);
        }

        return splits;
    }

    @Override
    public RecordReader<ImmutableBytesWritable, Result> getRecordReader(InputSplit inputSplit, JobConf entries, Reporter reporter) throws IOException {
        if (!(inputSplit instanceof HBaseTableSplitRegional))
            throw new IOException("Table Split is not type HBaseTableSplitRegional");

        HBaseTableSplitRegional tSplit = (HBaseTableSplitRegional)inputSplit;

        LOG.info("REGIONAL SPLIT -> " + tSplit);

        HBaseRecordReaderRegional trr = new HBaseRecordReaderRegional();

        HBaseConfigUtils.setRecordReaderParms(trr, tSplit);

        trr.setHTable(this.table);
        trr.setInputColumns(this.inputColumns);
        trr.setRowFilter(this.rowFilter);

        trr.init(tSplit);

        return trr;
    }

    private HBaseTableSplitRegional[] convertToRegionalSplitArray(
            HBaseTableSplitGranular[] splits) throws IOException {

        if (splits == null)
            throw new IOException("The list of splits is null => " + splits);

        HashMap<String, HBaseTableSplitRegional> regionSplits = new HashMap<String, HBaseTableSplitRegional>();

        for (HBaseTableSplitGranular hbt : splits) {
            HBaseTableSplitRegional mis = null;
            if (regionSplits.containsKey(hbt.getRegionName())) {
                mis = regionSplits.get(hbt.getRegionName());
            } else {
                regionSplits.put(hbt.getRegionName(), new HBaseTableSplitRegional(
                        hbt.getRegionLocation(), hbt.getRegionName()));
                mis = regionSplits.get(hbt.getRegionName());
            }

            mis.addSplit(hbt);
            regionSplits.put(hbt.getRegionName(), mis);
        }

//        for(String region : regionSplits.keySet() ) {
//            regionSplits.get(region)
//        }

        Collection<HBaseTableSplitRegional> outVals = regionSplits.values();

        LOG.debug("".format("Returning array of splits : %s", outVals));

        if (outVals == null)
            throw new IOException("The list of multi input splits were null");

        return outVals.toArray(new HBaseTableSplitRegional[outVals.size()]);
    }

}
