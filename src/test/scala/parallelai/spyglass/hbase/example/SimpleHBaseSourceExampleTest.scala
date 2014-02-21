package parallelai.spyglass.hbase.example

import org.junit.runner.RunWith
import com.twitter.scalding.{JobTest, TupleConversions}
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner
import org.slf4j.LoggerFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import cascading.tuple.{Tuple, Fields}
import org.apache.hadoop.hbase.util.Bytes
import scala._
import com.twitter.scalding.Tsv
import parallelai.spyglass.hbase.HBaseSource
import parallelai.spyglass.hbase.HBaseConstants.SourceMode

/**
 * Example of how to define tests for HBaseSource
 */
@RunWith(classOf[JUnitRunner])
class SimpleHBaseSourceExampleTest extends FunSpec with TupleConversions {

  val output = "/tmp/testOutput"

  val log = LoggerFactory.getLogger(this.getClass.getName)

  val sampleData = List(
    List("1", "kk1", "pp1"),
    List("2", "kk2", "pp2"),
    List("3", "kk3", "pp3")
  )

  JobTest("parallelai.spyglass.hbase.example.SimpleHBaseSourceExample")
    .arg("test", "")
    .arg("app.conf.path", "app.conf")
    .arg("output", output)
    .arg("debug", "true")
    .source[Tuple](
    new HBaseSource(
      "table_name",
      "quorum_name:2181",
      new Fields("key"),
      List("column_family"),
      List(new Fields("column_name1", "column_name2")),
      sourceMode = SourceMode.GET_LIST, keyList = List("1", "2", "3")),
    sampleData.map(l => new Tuple(l.map(s => {new ImmutableBytesWritable(Bytes.toBytes(s))}):_*)))
    .sink[Tuple](Tsv(output format "get_list")) {
      outputBuffer =>
        log.debug("Output => " + outputBuffer)

        it("should return the test data provided.") {
          println("outputBuffer.size => " + outputBuffer.size)
          assert(outputBuffer.size === 3)
        }
    }
    .run
    .finish

}
