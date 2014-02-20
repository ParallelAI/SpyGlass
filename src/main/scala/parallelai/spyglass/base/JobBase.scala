package parallelai.spyglass.base

import com.twitter.scalding.Job
import com.twitter.scalding.Args
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.filecache.DistributedCache
import com.twitter.scalding.HadoopMode
import com.typesafe.config.ConfigFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.twitter.scalding.NullSource
import parallelai.spyglass.base._
import com.twitter.scalding.Mode

class JobBase(args: Args) extends Job(args) {
  def getOrElseString(key: String, default: String): String = {
    args.m.getOrElse[List[String]](key, List(default)).head
  }
   
  def getOrElseList(key: String, default: List[String]): List[String] = {
    args.m.getOrElse[List[String]](key, default)
  }   

  def getString(key: String): String = {
    args.m.get(key) match {
      case Some(v) => v.head
      case None => sys.error(String.format("Argument [%s] - NOT FOUND", key))
    }
  }
  
  def getList(key: String): List[String] = {
    args.m.get(key) match {
      case Some(v) => v
      case None => sys.error(String.format("Argument [%s] - NOT FOUND", key))
    }
  }
  
  def getJobConf(): Configuration = {
	AppConfig.jobConfig
  }  

  
  val appConfig = ConfigFactory.parseFile(new java.io.File(getString("app.conf.path")))
  
  val log = LoggerFactory.getLogger(getOrElseString("app.log.name", this.getClass().getName()))
  
  def modeString(): String = {
      Mode.getMode(args) match {
        case x:HadoopMode  => "--hdfs"
        case _ => "--local"
    }
  }
  
  // Execute at instantiation
  Mode.getMode(args) match {
    case x:HadoopMode => {
      log.info("In Hadoop Mode")
      JobLibLoader.loadJars(getString("job.lib.path"), AppConfig.jobConfig);
    }
    case _ => {
      log.info("In Local Mode")
    }
  }
  
  def registerNullSourceSinkTaps(): Unit = {
    val expectedSampleEndToEndOutput = List(("", ""),("", ""),("", ""))
	val sourceTap = NullSource
			.writeFrom(expectedSampleEndToEndOutput)
  }
}

object AppConfig {
  implicit var jobConfig : Configuration = new Configuration()
}