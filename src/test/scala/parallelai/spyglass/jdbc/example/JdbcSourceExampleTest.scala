package parallelai.spyglass.jdbc.example

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import com.twitter.scalding.{Tsv, JobTest, TupleConversions}
import org.slf4j.LoggerFactory
import cascading.tuple.{Tuple, Fields}
import parallelai.spyglass.jdbc.JDBCSource

/**
 * Simple example of JDBCSource testing
 */
@RunWith(classOf[JUnitRunner])
class JdbcSourceExampleTest extends FunSpec with TupleConversions {

  val output = "src/test/resources/outputs/testOutput"

  val log = LoggerFactory.getLogger(this.getClass.getName)

  val sampleData = List(
    (1, "c11", "c12", 11),
    (2, "c21", "c22", 22)
  )

  JobTest("parallelai.spyglass.jdbc.example.JdbcSourceExample")
    .arg("local", "")
    .arg("app.conf.path", "app.conf")
    .arg("output", output)
    .arg("debug", "true")
    .source(
    new JDBCSource(
      "db_name",
      "com.mysql.jdbc.Driver",
      "jdbc:mysql://<hostname>:<port>/<db_name>?zeroDateTimeBehavior=convertToNull",
      "user",
      "password",
      List("KEY_ID", "COL1", "COL2", "COL3"),
      List("bigint(20)", "varchar(45)", "varchar(45)", "bigint(20)"),
      List("key_id"),
      new Fields("key_id", "col1", "col2", "col3")), sampleData)
    .sink[Tuple](Tsv(output.format("get_list"))) {
      outputBuffer =>
        log.debug("Output => " + outputBuffer)

        it("should return the mock data provided.") {
          assert(outputBuffer.size === 2)
        }
    }
    .run
    .finish


}
