package parallelai.spyglass.base

import org.apache.hadoop.conf.Configuration
import com.twitter.scalding.Tool
import org.apache.hadoop
 
object JobRunner {
  def main(args : Array[String]) {
    val conf: Configuration = new Configuration
    
    // TODO replace println with logging
    if (args.contains("--heapInc")) {
	    println("Setting JVM Memory/Heap Size for every child mapper and reducer.");
	    val jvmOpts = "-Xmx4096m -XX:+PrintGCDetails -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=50"
	    println("**** JVM Options : " + jvmOpts )
	    conf.set("mapred.child.java.opts", jvmOpts);
    }
     
    AppConfig.jobConfig = conf
     
    hadoop.util.ToolRunner.run(conf, new Tool, args);
  }
}