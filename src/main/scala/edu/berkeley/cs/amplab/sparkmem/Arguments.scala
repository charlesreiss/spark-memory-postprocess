package edu.berkeley.cs.amplab.sparkmem

import org.apache.spark.SparkConf

private[sparkmem]
class Arguments(conf: SparkConf, args: Array[String]) {
  import Util.stringToBytes

  var logDir: String = null
  var jsonFile: String = null
  var rddTrace: String = null
  var machineReadable: Boolean = false
  var skipStacks: Boolean = false
  var skipStacksExceptRDD: Boolean = false
  var debug: Boolean = false
  var makeConfig: Boolean = false
  var targetWorkers: Int = 0
  var targetCoresPerWorker: Int = 0
  var targetMemoryPerWorker: Long = 0

  var tasksInOrder = false
  var consolidateRDDs = false

  parse(args.toList)

  private def parse(args: List[String]): Unit = {
    // System.err.println(s"args = $args")
    args match {
      case ("--logDir" | "-l") :: value :: tail =>
        logDir = value
        parse(tail)

      case ("--jsonFile" | "-f") :: value :: tail =>
        jsonFile = value
        parse(tail)

      case "--rddTrace" :: value :: tail =>
        rddTrace = value
        parse(tail)

      case "--machineReadable" :: tail =>
        machineReadable = true
        parse(tail)

      case "--tasksInOrder" :: tail =>
        tasksInOrder = true
        parse(tail)

      case "--consolidateRDDs" :: tail =>
        consolidateRDDs = true
        parse(tail)

      case "--makeConfig" :: tail =>
        makeConfig = true
        parse(tail)

      case "--targetWorkers" :: value :: tail =>
        targetWorkers = value.toInt
        parse(tail)

      case "--targetCoresPerWorker" :: value :: tail =>
        targetCoresPerWorker = value.toInt
        parse(tail)

      case "--targetMemoryPerWorker" :: value :: tail =>
        targetMemoryPerWorker = stringToBytes(value)
        parse(tail)

      case "--skipStacks" :: tail =>
        skipStacks = true
        parse(tail)

      case "--skipStacksExceptRDD" :: tail =>
        skipStacksExceptRDD = true
        parse(tail)

      case "--debug" :: tail =>
        debug = true
        parse(tail)

      case Nil =>

      case _ =>
        printUsageAndExit(1)
    }
  }

  def printUsageAndExit(exitCode: Int) {
    System.err.println(
      """
      |Usage: ParseLogs [options]
      |
      |Options:
      |  --jsonFile FILE
      |    FILE to use to store analysis results. If --logDir is specified, output will go here;
      |    otherwise input will be read from here.
      |
      | Analyzing event logs:
      |  --logDir LOG-DIRECTORY
      |    Location of the application in question's output.
      |  --rddTrace OUTPUT-FILE
      |    Location to write raw RDD access trace (for debugging)
      |  --consolidateRDDs
      |    Treat all partitions of an RDD as a unit for the purpose of cache analyses.
      |  --tasksInOrder
      |    Analyze as if tasks are exectued serially in task ID order. (Default: as if tasks are
      |    executed serially in task completion order.)
      |
      | Creating potential config files:
      |  --makeConfig
      |    Output a (partial) spark configuration properties file.
      |  --targetMemoryPerWorker MEMORY
      |    Amount of memory to assume per node for generating Spark config file.
      |    Either this or targetWorkers must be specified. Output file will
      |    indicate suggested number of workers in a comment.
      |    MEMORY should be a string like 1.24g or 1234m.
      |  --targetWorkers NODES
      |    Number of workers to assume for generating Spark config file.
      |    Either this or targetMemoryPerWorker must be specified. Output file
      |    will indicate memory requirement in a comment.
      |  --targetCoresPerWorker CORES
      |    Number of cores to assume for generating Spark config file
      |  --machineReadable
      |    Write raw data in a machine readable format (for graphs)
      |  --skipStacks
      |    Skip stack analyses.
      |  --debug
      |    Enable debug logging.
      """.stripMargin)
    System.exit(exitCode)
  }
}
