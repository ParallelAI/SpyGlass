package parallelai.spyglass.hbase;

import org.apache.hadoop.conf.Configuration;

public class HBaseConstants {

    public enum SourceMode {
        EMPTY,
        SCAN_ALL,
        SCAN_RANGE,
        GET_LIST;
    }

    public enum SplitType {
        GRANULAR,
        REGIONAL;
    }

    public static final String START_KEY = "hbase.%s.startkey";
    public static final String STOP_KEY = "hbase.%s.stopkey";
    public static final String SOURCE_MODE = "hbase.%s.source.mode";
    public static final String KEY_LIST = "hbase.%s.key.list";
    public static final String VERSIONS = "hbase.%s.versions";
    public static final String USE_SALT = "hbase.%s.use.salt";
    public static final String SALT_PREFIX = "hbase.%s.salt.prefix";

    public static final String SINK_MODE = "hbase.%s.sink.mode";
}