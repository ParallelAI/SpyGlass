package parallelai.spyglass.jdbc;

import org.apache.hadoop.conf.Configuration;

public class JDBCConstants {
  
  public enum JdbcSourceMode {
    SELECT,
    SELECT_WITH_PARTITIONS,
    SELECT_WITH_BUCKETS;
  }

  public enum JdbcSinkMode {
	INSERT,
	UPDATE,
	UPSERT,
	BATCH_INSERT,
	BATCH_UPSERT,
	MULTI_ROW_INSERT,
	MULTI_ROW_UPSERT;
  }
  
  public static final String START_KEY = "jdbc.%s.startkey";
  public static final String STOP_KEY = "jdbc.%s.stopkey"; 
  public static final String SOURCE_MODE = "jdbc.%s.source.mode";
  public static final String KEY_LIST = "jdbc.%s.key.list";
  public static final String VERSIONS = "jdbc.%s.versions";

}
