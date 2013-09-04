package parallelai.spyglass.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.util.StringUtils;

import java.util.Collection;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: chand_000
 * Date: 29/08/13
 * Time: 12:43
 * To change this template use File | Settings | File Templates.
 */
public abstract class HBaseInputFormatBase implements InputFormat<ImmutableBytesWritable, Result>, JobConfigurable {

    private final Log LOG = LogFactory.getLog(HBaseInputFormatBase.class);

    protected final String id = UUID.randomUUID().toString();
    protected byte[][] inputColumns;
    protected HTable table;
    protected Filter rowFilter;

    public static final String COLUMN_LIST = "hbase.tablecolumns";

    /**
     * Use this jobconf param to specify the input table
     */
    protected static final String INPUT_TABLE = "hbase.inputtable";

    protected String startKey = null;
    protected String stopKey = null;

    protected HBaseConstants.SourceMode sourceMode = HBaseConstants.SourceMode.EMPTY;
    protected TreeSet<String> keyList = null;
    protected int versions = 1;
    protected boolean useSalt = false;
    protected String prefixList = HBaseSalter.DEFAULT_PREFIX_LIST;



    @Override
    public void configure(JobConf job) {
        String tableName = getTableName(job);
        String colArg = job.get(COLUMN_LIST);
        String[] colNames = colArg.split(" ");
        byte[][] m_cols = new byte[colNames.length][];
        for (int i = 0; i < m_cols.length; i++) {
            m_cols[i] = Bytes.toBytes(colNames[i]);
        }
        setInputColumns(m_cols);

        try {
            setHTable(new HTable(HBaseConfiguration.create(job), tableName));
        } catch (Exception e) {
            LOG.error("************* Table could not be created");
            LOG.error(StringUtils.stringifyException(e));
        }

        LOG.debug("Entered : " + this.getClass() + " : configure()");

        useSalt = job.getBoolean(
                String.format(HBaseConstants.USE_SALT, getTableName(job)), false);
        prefixList = job.get(
                String.format(HBaseConstants.SALT_PREFIX, getTableName(job)),
                HBaseSalter.DEFAULT_PREFIX_LIST);

        sourceMode = HBaseConstants.SourceMode.valueOf(job.get(String.format(
                HBaseConstants.SOURCE_MODE, getTableName(job))));

        LOG.info(String.format("GOT SOURCE MODE (%s) as (%s) and finally", String
                .format(HBaseConstants.SOURCE_MODE, getTableName(job)), job
                .get(String.format(HBaseConstants.SOURCE_MODE, getTableName(job))),
                sourceMode));

        switch (sourceMode) {
            case SCAN_RANGE:
                LOG.debug("HIT SCAN_RANGE");

                startKey = getJobProp(job,
                        String.format(HBaseConstants.START_KEY, getTableName(job)));
                stopKey = getJobProp(job,
                        String.format(HBaseConstants.STOP_KEY, getTableName(job)));

                LOG.debug(String.format("Setting start key (%s) and stop key (%s)",
                        startKey, stopKey));
                break;

            case GET_LIST:
                LOG.debug("HIT GET_LIST");

                Collection<String> keys = job.getStringCollection(String.format(
                        HBaseConstants.KEY_LIST, getTableName(job)));
                keyList = new TreeSet<String>(keys);

                versions = job.getInt(
                        String.format(HBaseConstants.VERSIONS, getTableName(job)), 1);

                LOG.debug("GOT KEY LIST : " + keys);
                LOG.debug(String.format("SETTING key list (%s)", keyList));

                break;

            case EMPTY:
                LOG.info("HIT EMPTY");

                sourceMode = HBaseConstants.SourceMode.SCAN_ALL;
                break;

            default:
                LOG.info("HIT DEFAULT");

                break;
        }
    }

    /* Getters & Setters */
    protected HTable getHTable() {
        return this.table;
    }

    protected void setHTable(HTable ht) {
        this.table = ht;
    }

    protected void setInputColumns(byte[][] ic) {
        this.inputColumns = ic;
    }

    protected void setJobProp(JobConf job, String key, String value) {
        if (job.get(key) != null)
            throw new RuntimeException(String.format(
                    "Job Conf already has key [%s] with value [%s]", key,
                    job.get(key)));
        job.set(key, value);
    }

    protected String getJobProp(JobConf job, String key) {
        return job.get(key);
    }

    public static void setTableName(JobConf job, String tableName) {
        // Make sure that table has not been set before
        String oldTableName = getTableName(job);
        if (oldTableName != null) {
            throw new RuntimeException("table name already set to: '"
                    + oldTableName + "'");
        }

        job.set(INPUT_TABLE, tableName);
    }

    public static String getTableName(JobConf job) {
        return job.get(INPUT_TABLE);
    }

    protected void setParms(HBaseRecordReaderBase trr) {

    }
}
