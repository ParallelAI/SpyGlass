package parallelai.spyglass.jdbc.testing

import com.twitter.scalding._
import cascading.tuple.Fields
import parallelai.spyglass.base.JobBase
import parallelai.spyglass.jdbc.JDBCSource

/**
 * Compares whether two tables have the same data or not writing to HDFS the ids of the records that don't match.
 * Now hardcoded for Skybet betdetail summation sample.
 * To run it:
 * bskyb.commons.scalding.base.JobRunner bskyb.commons.skybase.jdbc.testing.TablesComparison \
 *    --app.conf.path /projects/application-hadoop.conf --hdfs  \
 *    --job.lib.path file:///home/gfe01/IdeaProjects/commons/commons.hbase.skybase/alternateLocation
 * @param args
 */
class TablesComparison(args: Args) extends JobBase(args) {

  implicit val implicitArgs: Args = args
  val conf = appConfig

  val jdbcSink = new JDBCSource(
    "table_name",
    "com.mysql.jdbc.Driver",
    "jdbc:mysql://<hostname>:<port>/<db_name>",
    "skybet_user",
    "zkb4Uo{C8",
    Array[String]("BETLEG_ID", "CR_DATE", "EV_MKT_ID", "CUST_ID", "SET_DATETIME", "BET_DATETIME", "STATUS", "SOURCE", "BET_TYPE", "AFF_NAME", "CURRENCY_CODE", "BET_ID", "LEG_ID", "RECEIPT_NO", "STAKE", "REFUND", "WINNINGS", "PROFIT", "STAKE_GBP", "REFUND_GBP", "WINNINGS_GBP", "PROFIT_GBP", "NUM_SELS", "EXCH_RATE", "ACCT_NO", "BET_IP_ADDRESS", "NUM_DRAWS", "EXTRACT_DATE", "BET_TIME", "BET_DATE_TIME", "SET_DATE_TIME", "BET_DATE_MONTH", "SET_TIME_KEY", "BET_TIME_KEY", "SET_TIME", "SET_DATE", "BET_DATE", "PART_NO", "MARKET_SORT", "TAG", "PLACED_IN_RUNNING", "MAX_STAKE_SCALE", "ODDS_NUM", "ODDS_DEN", "EV_OC_ID", "USER_CLIENT_ID"),
    Array[String]("bigint(20)", "varchar(45)", "varchar(45)", "bigint(20)", "varchar(45)", "varchar(45)", "char(1)", "varchar(45)", "char(5)", "varchar(45)", "char(3)", "bigint(20)", "bigint(20)", "varchar(24)", "decimal(12,2)", "decimal(12,2)", "decimal(12,2)", "decimal(12,2)", "decimal(12,2)", "decimal(12,2)", "decimal(12,2)", "decimal(12,2)", "smallint(6)", "decimal(12,2)", "varchar(45)", "char(15)", "int(11)", "varchar(45)", "varchar(45)", "varchar(45)", "varchar(45)", "varchar(45)", "varchar(45)", "varchar(45)", "varchar(45)", "varchar(45)", "varchar(45)", "int(11)", "varchar(45)", "varchar(45)", "char(1)", "double(10,5)", "int(11)", "int(11)", "bigint(20)", "varchar(45)"),
    Array[String]("betleg_id"),
    new Fields("betleg_id", "cr_date", "ev_mkt_id", "cust_id", "set_datetime", "bet_datetime", "status", "source", "bet_type", "aff_name", "currency_code", "bet_id", "leg_id", "receipt_no", "stake", "refund", "winnings", "profit", "stake_gbp", "refund_gbp", "winnings_gbp", "profit_gbp", "num_sels", "exch_rate", "acct_no", "bet_ip_address", "num_draws", "extract_date", "bet_time", "bet_date_time", "set_date_time", "bet_date_month", "set_time_key", "bet_time_key", "set_time", "set_date", "bet_date", "part_no", "market_sort", "tag", "placed_in_running", "max_stake_scale", "odds_num", "odds_den", "ev_oc_id", "user_client_id"),
    Array[String]("BETLEG_ID"),
    Array[String]("BETLEG_ID"),
    new Fields("betleg_id")
  )
    .read
    .insert('name, "betdetail")
    .project('bet_id, 'part_no, 'leg_id)

  //
  // .write(new TextLine("testJDBCComparator/compare1"))

  val jdbcSource2 = new JDBCSource(
    "skybet_midas_bet_detail",
    "com.mysql.jdbc.Driver",
    "jdbc:mysql://mysql01.prod.bigdata.bskyb.com:3306/skybet_db?zeroDateTimeBehavior=convertToNull",
    "skybet_user",
    "zkb4Uo{C8",
    Array[String]("BET_ID", "BET_TYPE_ID", "RECEIPT_NO", "NUM_SELS", "NUM_LINES", "BET_CHANNEL_CODE", "MOBILE_CLIENT_ID", "BET_AFFILIATE_ID", "BET_IP_ADDRESS", "LEG_ID", "LEG_TYPE", "OUTCOME_ID", "ACCT_ID", "BET_PLACED_DATETIME", "BET_PLACED_DATE", "BET_PLACED_TIME", "BET_SETTLED_DATETIME", "BET_SETTLED_DATE", "BET_SETTLED_TIME", "BET_STATUS", "STAKE", "REFUND", "'RETURN'", "PROFIT", "CURRENCY_TYPE_KEY", "EXCH_RATE", "STAKE_GBP", "REFUNDS_GBP", "RETURN_GBP", "PROFIT_GBP", "MARKET_TAG", "MARKET_SORT", "PLACED_IN_RUNNING", "ODDS_NUM", "ODDS_DEN", "BETLEG_ID", "PART_NO"),
    Array[String]("bigint(20)", "varchar(16)", "varchar(32)", "int(10)", "int(10)", "char(1)", "varchar(32)", "varchar(32)", "varchar(15)", "int(11)", "char(1)", "bigint(20)", "bigint(20)", "datetime", "date", "time", "datetime", "date", "time", "varchar(32)", "decimal(8,3)", "decimal(8,3)", "decimal(8,3)", "decimal(8,3)", "varchar(32)", "decimal(8,3)", "decimal(8,3)", "decimal(8,3)", "decimal(8,3)", "decimal(8,3)", "varchar(32)", "varchar(32)", "varchar(32)", "int(11)", "int(11)"),
    Array[String]("bet_id"),
    new Fields("bet_id", "bet_type_id", "receipt_no", "num_sels", "num_lines", "bet_channel_code", "mobile_client_id", "bet_affiliate_id", "bet_ip_address", "leg_id", "leg_type", "outcome_id", "acct_id", "bet_placed_datetime", "bet_placed_date", "bet_placed_time", "bet_settled_datetime", "bet_settled_date", "bet_settled_time", "bet_status", "stake", "refund", "return", "profit", "currency_type_key", "exch_rate", "stake_gbp", "refunds_gbp", "return_gbp", "profit_gbp", "market_tag", "market_sort", "placed_in_running", "odds_num", "odds_den", "betleg_id", "part_no")

  )
    .read
    .insert('name, "sample")
    .project('bet_id, 'part_no, 'leg_id)

  val uPipe = jdbcSink ++ jdbcSource2
  uPipe
    .groupBy('bet_id, 'part_no, 'leg_id) {
    _.size
  }.filter('size) {
    x: Int => x != 2
  }
    .write(new TextLine("testJDBCComparator/result"))
}