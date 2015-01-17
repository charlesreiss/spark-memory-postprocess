package edu.berkeley.cs.amplab.sparkmem

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.scheduler.ReplayListenerBus

object ParseLogs {
  def main(rawArgs: Array[String]) {
    val conf = new SparkConf
    val args = new ParseLogArguments(conf, rawArgs)
    val fs = FileSystem.get(new Path(args.logDir).toUri, SparkHadoopUtil.get.newConfiguration(conf))
    val replayBus = ReplayListenerBus.fromLogDirectory(new Path(args.logDir), fs)
    val blockAccessListener = new BlockAccessListener
    replayBus.addListener(blockAccessListener)
    // FIXME: AddListener
    replayBus.replay()
    println("UsageInfo = " + blockAccessListener.usageInfo)
  }
}
