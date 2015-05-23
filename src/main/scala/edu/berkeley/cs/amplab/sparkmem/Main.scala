package edu.berkeley.cs.amplab.sparkmem

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.io.CompressionCodec
import org.apache.spark.scheduler.ReplayListenerBus
import org.apache.spark.scheduler.EventLoggingListener

import java.io.{BufferedInputStream, File, PrintWriter}

import org.apache.log4j.BasicConfigurator

import org.apache.log4j.{Logger => L4JLogger, Level => L4JLevel}

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import scala.io.Source

object Main extends Logging {
  private def playLogOnce(conf: SparkConf, args: Arguments, listener: BlockAccessListener) {
    (args.logFile, args.logDir) match {
      case (Some(logFile), None) => {
        val fs = FileSystem.get(new Path(logFile).toUri, SparkHadoopUtil.get.newConfiguration(conf))
        val logInput = EventLoggingListener.openEventLog(new Path(logFile), fs)
        val replayBus = new ReplayListenerBus
        replayBus.addListener(listener)
        try {
          replayBus.replay(logInput, logFile)
        } finally {
          logInput.close()
        }
      }
      case (None, Some(logDir)) => {
        val fs = FileSystem.get(new Path(logDir).toUri, SparkHadoopUtil.get.newConfiguration(conf))
        var logFile: Path = null
        var logCodecName: Option[String] = None
        // From FsHistoryProvider.openLegacyLog
        val CODEC_PREFIX = "COMPRESSION_CODEC_"
        fs.listStatus(new Path(logDir)).foreach { child =>
          child.getPath().getName() match {
            case name if name.startsWith("EVENT_LOG_") =>
              logFile = child.getPath()
            case codec if codec.startsWith(CODEC_PREFIX) =>
              logCodecName = Some(codec.substring(CODEC_PREFIX.length()))
            case _ => {}
          }
        }
        val codec = logCodecName match {
          case None => None
          case Some("snappy") => Some(new org.apache.spark.io.SnappyCompressionCodec(conf))
          case _ => throw new IllegalArgumentException("Unrecognized codec" + logCodecName)
        }
        val logInputRaw = new BufferedInputStream(fs.open(logFile))
        val logInput = codec.map(c => c.compressedInputStream(logInputRaw)).getOrElse(logInputRaw)
        val replayBus = new ReplayListenerBus
        replayBus.addListener(listener)
        try {
          replayBus.replay(logInput, logDir)
        } finally {
          logInput.close()
        }
      }
      case (_, _) => throw new IllegalArgumentException
    }
  }

  private def nanosecondsToStr(nanos: Long): String = {
    val MILLISECOND = 1000L * 1000L
    val SECOND = 1000L * 1000L * 1000L
    if (nanos > 10L * SECOND) {
      return f"${nanos.toDouble / SECOND}%.2f s"
    } else {
      return f"${nanos.toDouble / MILLISECOND}%.2f ms"
    }
  }

  private def usageInfoFromEventLog(conf: SparkConf, args: Arguments): UsageInfo = {
    val blockAccessListener = new BlockAccessListener
    blockAccessListener.skipExtraStacks = args.skipStacks || args.skipStacksExceptRDD
    blockAccessListener.skipRDDStack = args.skipStacks && !args.skipStacksExceptRDD
    blockAccessListener.consolidateRDDs = args.consolidateRDDs
    blockAccessListener.sortTasks = args.tasksInOrder
    args.rddTrace.foreach { rddTracePath => 
      blockAccessListener.recordLogFile = new PrintWriter(new File(rddTracePath))
    }

    val startTime = System.nanoTime()
    // preprocesing step to get all block sizes
    blockAccessListener.skipProcessTasks = true
    playLogOnce(conf, args, blockAccessListener)
    val endFirstPassTime = System.nanoTime()
    // second step to use block sizes
    blockAccessListener.skipProcessTasks = false
    playLogOnce(conf, args, blockAccessListener)
    val endTime = System.nanoTime()

    Option(blockAccessListener.recordLogFile).foreach(_.close)

    logInfo(s"Processed log in ${nanosecondsToStr(endTime - startTime)} " +
            s"(first pass ${nanosecondsToStr(endFirstPassTime - startTime)})")

    blockAccessListener.usageInfo
  }

  def usageInfoFromJson(args: Arguments): UsageInfo = {
    val theJson: String = Source.fromFile(args.jsonFile.get).mkString
   
    UsageInfo.fromJson(parse(theJson))
  }

  def main(rawArgs: Array[String]) {
    val conf = new SparkConf
    val args = new Arguments(conf, rawArgs)
    args.sanityCheck()
    BasicConfigurator.configure()
    if (args.debug) {
      L4JLogger.getRootLogger.setLevel(L4JLevel.DEBUG)
    }

    val usageInfo =
      if (args.haveLog)
        usageInfoFromEventLog(conf, args)
      else
        usageInfoFromJson(args)

    if (args.haveLog) {
      args.jsonFile match {
        case Some(jsonFile) => {
          val writer = new PrintWriter(new File(jsonFile))
          writer.write(usageInfo.toJsonString)
          writer.close()
        }
      }
    }

    if (args.makeConfig) {
      val settings = args.makeConfigSettingsFile.map(
        f => ProposedConfigSettings.fromJson(Source.fromFile(new File(f)).mkString)
      ).getOrElse(ProposedConfigSettings.DEFAULT)
      assert(args.targetWorkers > 0 || args.targetMemoryPerWorker > 0)
      val config = if (args.targetWorkers > 0) {
          ProposedConfig.forWorkerCount(usageInfo, args.targetCoresPerWorker, args.targetWorkers, settings)
        } else {
          assert(args.targetMemoryPerWorker > 0)
          ProposedConfig.forWorkerSize(usageInfo, args.targetCoresPerWorker, args.targetMemoryPerWorker,
                                       settings)
        }
      println(config.configFile)
    } else if (args.csvOutput) {
      println(usageInfo.csvHeader)
      for (i <- 1 to 16) {
        println(usageInfo.csvLine(i))
      }
    }
  }
}
