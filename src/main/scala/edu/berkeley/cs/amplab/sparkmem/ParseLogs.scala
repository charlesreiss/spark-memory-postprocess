package edu.berkeley.cs.amplab.sparkmem

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.scheduler.ReplayListenerBus

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
    val replayBus = ReplayListenerBus.fromLogDirectory(new Path(args.logDir), fs)
    val blockAccessListener = new BlockAccessListener
    blockAccessListener.skipExtraStacks = args.skipStacks || args.skipStacksExceptRDD
    blockAccessListener.skipRDDStack = args.skipStacks && !args.skipStacksExceptRDD
    Option(args.rddTrace).foreach { rddTracePath => 
      blockAccessListener.recordLogFile = new PrintWriter(new File(rddTracePath))
    }
    replayBus.addListener(blockAccessListener)
    // FIXME: AddListener
    replayBus.replay()
    Option(blockAccessListener.recordLogFile).foreach(_.close)
    if (args.machineReadable) {
      val usageInfo = blockAccessListener.usageInfo
      println(usageInfo.csvHeader)
      for (i <- 1 to 16) {
        println(usageInfo.csvLine(i))
      }
    } else {
      println("RDD block count = " + blockAccessListener.rddBlockCount)
      println("RDD size incr = " + blockAccessListener.sizeIncrements)
      println("UsageInfo (15) = " + blockAccessListener.usageInfo.summary(15))
      println("UsageInfo (4) = " + blockAccessListener.usageInfo.summary(4))
    }
  }
}
