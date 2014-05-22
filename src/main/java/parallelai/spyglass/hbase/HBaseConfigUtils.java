package parallelai.spyglass.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * Utility class that sets the parameters of the record reader of a table split
 */
public class HBaseConfigUtils {
    static final Log LOG = LogFactory.getLog(HBaseConfigUtils.class);

    public static void setRecordReaderParms(HBaseRecordReaderBase trr, HBaseTableSplitBase tSplit) throws IOException {
        switch (tSplit.getSourceMode()) {
            case SCAN_ALL:
            case SCAN_RANGE: {
                LOG.debug(String.format(
                        "For split [%s] we have start key (%s) and stop key (%s)",
                        tSplit, tSplit.getStartRow(), tSplit.getEndRow()));

                trr.setStartRow(tSplit.getStartRow());
                trr.setEndRow(tSplit.getEndRow());
                trr.setEndRowInclusive(tSplit.getEndRowInclusive());
                trr.setUseSalt(tSplit.getUseSalt());
                trr.setTimestamp(tSplit.getTimestamp());
            }

            break;

            case GET_LIST: {
                LOG.debug(String.format("For split [%s] we have key list (%s)",
                        tSplit, tSplit.getKeyList()));

                trr.setKeyList(tSplit.getKeyList());
                trr.setVersions(tSplit.getVersions());
                trr.setUseSalt(tSplit.getUseSalt());
            }

            break;

            default:
                throw new IOException("Unknown source mode : "
                        + tSplit.getSourceMode());
        }

        trr.setSourceMode(tSplit.getSourceMode());
    }

}
