package parallelai.spyglass.jdbc.testing

import com.twitter.scalding.TextLine
import com.twitter.scalding.Args
import com.twitter.scalding.Tsv
import com.twitter.scalding.mathematics.Matrix._
import scala.math._
import scala.math.BigDecimal.javaBigDecimal2bigDecimal
import cascading.tuple.Fields
import cascading.pipe.Pipe
import com.twitter.scalding.Osv
import parallelai.spyglass.base.JobBase
import parallelai.spyglass.jdbc.JDBCSource

class HdfsToJdbc (args: Args) extends JobBase(args) { 

	implicit val implicitArgs: Args = args
  
    val scaldingInputPath = getString("input.scalding")  
    log.info("Scalding sample input path => [" + scaldingInputPath + "]")
  
    val S_output = scaldingInputPath
    val fileType = getString("fileType")
    log.info("Input file type => " + fileType)
  
    val S_SCHEMA = List(
		  'key_id,    'col1, 'col2, 'col3
    )  
    	
	val url = "mysql01.domain"
	val dbName = "db"
	val tableName = "table"
	
	  
	val jdbcSource2 = new JDBCSource(
		"db_name",
		"com.mysql.jdbc.Driver",
		"jdbc:mysql://<hostname>:<port>/<db_name>?zeroDateTimeBehavior=convertToNull",
		"user",
		"password",
    List("KEY_ID", "COL1", "COL2", "COL3"),
    List( "bigint(20)" ,  "varchar(45)"  ,  "varchar(45)"  ,  "bigint(20)"),
    List("key_id"),
		new Fields("key_id",    "col1",    "col2",    "col3")
	)
  
    var piper:Pipe = null
    if (fileType equals("Tsv"))
    	piper = Tsv(S_output, S_SCHEMA).read
    else
    	piper = Osv(S_output, S_SCHEMA).read
	   
	val S_FLOW = 
    	Tsv(S_output, S_SCHEMA).read
    	.write(jdbcSource2)

}