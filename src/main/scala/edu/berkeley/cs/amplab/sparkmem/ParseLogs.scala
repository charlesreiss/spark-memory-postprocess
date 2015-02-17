package edu.berkeley.cs.amplab.sparkmem

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.scheduler.ReplayListenerBus

import java.io.{File, PrintWriter}

object ParseLogs {
  def main(rawArgs: Array[String]) {
    val conf = new SparkConf
    val args = new ParseLogArguments(conf, rawArgs)
    val fs = FileSystem.get(new Path(args.logDir).toUri, SparkHadoopUtil.get.newConfiguration(conf))
    val replayBus = ReplayListenerBus.fromLogDirectory(new Path(args.logDir), fs)
    val blockAccessListener = new BlockAccessListener
    Option(args.rddTrace).foreach { rddTracePath => 
      blockAccessListener.recordLogFile = new PrintWriter(new File(rddTracePath))
    }
    replayBus.addListener(blockAccessListener)
    // FIXME: AddListener
    replayBus.replay()
    Option(blockAccessListener.recordLogFile).foreach(_.close)
    println("RDD block count = " + blockAccessListener.rddBlockCount)
    println("RDD size incr = " + blockAccessListener.sizeIncrements)
    println("UsageInfo (15) = " + blockAccessListener.usageInfo.summary(15))
    println("UsageInfo (4) = " + blockAccessListener.usageInfo.summary(4))
  }
}
