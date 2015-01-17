package edu.berkeley.cs.amplab.sparkmem

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

private[sparkmem]
class ParseLogArguments(conf: SparkConf, args: Array[String]) {
  var logDir: String = null
  private var propertiesFile: String =  null

  parse(args.toList)

  private def parse(args: List[String]): Unit = {
    args match {
      case ("--logDir" | "-l") :: value :: tail =>
        logDir = value
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
      """.stripMargin)
    System.exit(exitCode)
  }
}
