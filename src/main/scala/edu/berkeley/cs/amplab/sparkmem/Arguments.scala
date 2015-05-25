package edu.berkeley.cs.amplab.sparkmem

import org.apache.spark.SparkConf

private[sparkmem]
class Arguments(conf: SparkConf, args: Array[String]) {
  import Util.stringToBytes

  var logDir: Option[String] = None
  var logFile: Option[String] = None
  def haveLog: Boolean = logDir.isDefined || logFile.isDefined
  var jsonFile: Option[String] = None
  var rddTrace: Option[String] = None
  var csvOutput: Boolean = false
  var skipStacks: Boolean = false
  var skipStacksExceptRDD: Boolean = false
  var debug: Boolean = false
  var makeConfig: Boolean = false
  var makeConfigProperties: Option[String] = None
  var targetWorkers: Int = 0
  var targetCoresPerWorker: Int = 0
  var targetMemoryPerWorker: Long = 0

  var tasksInOrder = false
  var consolidateRDDs = false

  def sanityCheck() {
    if (logDir.isDefined && logFile.isDefined) {
      System.err.println("Specify exactly one of --logDir and --logFile.")
      printUsageAndExit(1)
    } else if (!haveLog && !jsonFile.isDefined) {
      System.err.println("Either specify a log or a the json file resulting from a log.")
      printUsageAndExit(1)
    } else if (csvOutput && makeConfig) {
      System.err.println("Cannot combine --csvOutput with --makeConfig")
      printUsageAndExit(1)
    }
  }

  parse(args.toList)

  private def parse(args: List[String]): Unit = {
    // System.err.println(s"args = $args")
    args match {
      case ("--logFile" | "-l") :: value :: tail =>
        logFile = Some(value)
        parse(tail)

      case ("--logDir") :: value :: tail =>
        logDir = Some(value)
        parse(tail)

      case ("--jsonFile" | "-f") :: value :: tail =>
        jsonFile = Some(value)
        parse(tail)

      case "--rddTrace" :: value :: tail =>
        rddTrace = Some(value)
        parse(tail)

      case "--csvOutput" :: tail =>
        csvOutput = true
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

      case "--makeConfigProperties" :: value :: tail =>
        makeConfigProperties = Some(value)
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
        System.err.println(s"Unrecognized argument ${args.head}")
        printUsageAndExit(1)
    }
  }

  def printUsageAndExit(exitCode: Int) {
    System.err.println(
      """
      |Usage: ./run [options]
      |
      |Options:
      |  --jsonFile FILE
      |    FILE storing log analysis summary. If --logFile or --logFile is specified, output will
      |    go here; otherwise input will be read from here.
      |  --debug
      |    Enable debug logging.
      |
      | Analyzing event logs:
      |  --logFile LOG-FILE
      |    Single event log file with embedded metadata and extension indicating compression (Spark >= 1.4)
      |  --logDir LOG-DIRECTORY
      |    Event log directory with metadata files (Spark <= 1.3)
      |  --rddTrace OUTPUT-FILE
      |    Location to write raw RDD access trace (for debugging)
      |  --consolidateRDDs
      |    Treat all partitions of an RDD as a unit for the purpose of cache analyses.
      |  --tasksInOrder
      |    Analyze as if tasks are exectued serially in task ID order. (Default: as if tasks are
      |    executed serially in task completion order.)
      |  --skipStacks
      |    Skip stack analyses entirely. (For debugging; prevents getting useful recommendations.)
      |  --skipStacksExceptRDD
      |    Skip stack analyses except for RDDs entirely. (For debugging.)
      |
      | Creating potential config files:
      |  --makeConfig
      |    Output a (partial) spark configuration properties file.
      |  --makeConfigProperties FILE
      |    Properties file of settings besides targets specified below.
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
      |
      | Other output mode:
      |  --csvOutput
      |    Write raw data in a machine readable format (for graphs)
      """.stripMargin)
    System.exit(exitCode)
  }
}
