package edu.berkeley.cs.amplab.sparkmem

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.scheduler.ReplayListenerBus
import org.apache.spark.scheduler.EventLoggingListener

import java.io.{File, PrintWriter}

import org.apache.log4j.BasicConfigurator

import org.apache.log4j.{Logger => L4JLogger, Level => L4JLevel}

object ParseLogs {
  def main(rawArgs: Array[String]) {
    val conf = new SparkConf
    val args = new ParseLogArguments(conf, rawArgs)
    BasicConfigurator.configure()
    if (args.debug) {
      L4JLogger.getRootLogger.setLevel(L4JLevel.DEBUG)
    } 
    val fs = FileSystem.get(new Path(args.logDir).toUri, SparkHadoopUtil.get.newConfiguration(conf))
    val blockAccessListener = new BlockAccessListener
    blockAccessListener.skipExtraStacks = args.skipStacks || args.skipStacksExceptRDD
    blockAccessListener.skipRDDStack = args.skipStacks && !args.skipStacksExceptRDD
    Option(args.rddTrace).foreach { rddTracePath => 
      blockAccessListener.recordLogFile = new PrintWriter(new File(rddTracePath))
    }
    /* 1.3.0-memanalysis hack:
      val replayBus = ReplayListenerBus.fromLogDirectory(new Path(args.logDir), fs)
      replayBus.addListener(blockAccessListener)
      replayBus.replay()
    */
    val replayBus = new ReplayListenerBus
    val logInput = EventLoggingListener.openEventLog(new Path(args.logDir), fs)
    try {
      replayBus.replay(logInput, args.logDir)
    } finally {
      logInput.close()
    }

    Option(blockAccessListener.recordLogFile).foreach(_.close)
    if (args.makeConfig) {
      assert(!args.machineReadable)
      assert(args.targetWorkers > 0 || args.targetMemoryPerWorker > 0)
      val usageInfo = blockAccessListener.usageInfo
      val config = if (args.targetWorkers > 0) {
          ProposedConfig.forWorkerCount(usageInfo, args.targetCoresPerWorker, args.targetWorkers)
        } else {
          assert(args.targetMemoryPerWorker > 0)
          ProposedConfig.forWorkerSize(usageInfo, args.targetCoresPerWorker, args.targetMemoryPerWorker)
        }
      println(config.configFile)
    } else if (args.machineReadable) {
      val usageInfo = blockAccessListener.usageInfo
      println(usageInfo.csvHeader)
      for (i <- 1 to 16) {
        println(usageInfo.csvLine(i))
      }
    } else {
      args.printUsageAndExit(1)
    }
  }
}
