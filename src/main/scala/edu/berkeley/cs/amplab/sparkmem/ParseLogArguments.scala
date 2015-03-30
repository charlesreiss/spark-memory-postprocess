package edu.berkeley.cs.amplab.sparkmem

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

private[sparkmem]
class ParseLogArguments(conf: SparkConf, args: Array[String]) {
  var logDir: String = null
  var rddTrace: String = null
  var machineReadable: Boolean = false
  var skipStacks: Boolean = false
  private var propertiesFile: String =  null

  parse(args.toList)

  private def parse(args: List[String]): Unit = {
    System.err.println(s"args = $args")
    args match {
      case ("--logDir" | "-l") :: value :: tail =>
        logDir = value
        parse(tail)

      case "--rddTrace" :: value :: tail =>
        rddTrace = value
        parse(tail)

      case "--machineReadable" :: tail =>
        machineReadable = true
        parse(tail)

      case "--skipStacks" :: tail =>
        skipStacks = true
        parse(tail)

      case Nil =>

      case _ =>
        printUsageAndExit(1)
    }
  }

  private def printUsageAndExit(exitCode: Int) {
    System.err.println(
      """
      |Usage: ParseLogs [options]
      |
      |Options:
      |  --logDir LOG-DIRECTORY
      |    Location of the application in question's output.
      |  --rddTrace OUTPUT-FILE
      |    Location to write raw RDD access trace (for debugging)
      |  --machineReadable
      |    Write in machine readable format (CSV)
      |  --skipStacks
      |    Skip stack analyses.
      """.stripMargin)
    System.exit(exitCode)
  }
}
