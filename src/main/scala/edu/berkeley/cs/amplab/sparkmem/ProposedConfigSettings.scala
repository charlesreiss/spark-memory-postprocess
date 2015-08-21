package edu.berkeley.cs.amplab.sparkmem

import java.io.{File, FileInputStream}
import java.util.Properties

class ProposedConfigSettings(private val properties: Properties) {
  private def getLong(x: String, default: Long) = 
    Option(properties.getProperty(x)).map(_.toLong).getOrElse(default)
  private def getDouble(x: String, default: Double) = 
    Option(properties.getProperty(x)).map(_.toDouble).getOrElse(default)
  private def getBoolean(x: String, default: Boolean) =
    Option(properties.getProperty(x)).map(_.toBoolean).getOrElse(default)

  def gcBonus: Double = getDouble("gcBonus", 3.0 / 2.0)
  def assumedSlack: Double = getDouble("assumedSlack", 0.9)

  def minShuffleSizePerCore: Long = getLong("minShuffleSizePerCore", 16L * 1024L * 1024L)
  def minStorageSizePerCore: Long = getLong("minStorageSizePerCore", 16L * 1024L * 1024L)
  def minUnrollStorageSizePerCore: Long = getLong("minUnrollSizePerCore", 4L * 1024L * 1024L)

  def portionExtraStorage: Double = getDouble("portionExtraStorage", 0.45)
  def portionExtraUnrollStorage: Double = getDouble("portionExtraUnrollStorage", 0.1)
  def portionExtraShuffle: Double = getDouble("portionExtraShuffle", 0.25)
  def portionExtraUnassignedJvm: Double = getDouble("portionExtraUnassignedJvm", 0.05)

  def ignoreNonJvmSpace: Boolean = getBoolean("ignoreNonJvmSpace", false)

  def scalePartitionsBaseFactor: Double = getDouble("scalePartitionsBaseFactor", 1.0)
  def scalePartitionsBasedOnTasks: Boolean = getBoolean("scalePartitionsBasedOnTasks", false)
  def scalePartitionsBaseTaskCount: Double = getDouble("scalePartitionsBaseTaskCount", 1.0)

  def scalePartitionsFactorFor(taskCount: Double): Double = {
    val taskScale =
      if (scalePartitionsBasedOnTasks)
        scalePartitionsBaseTaskCount / taskCount
      else
        1.0
    taskScale * scalePartitionsBaseFactor
  }
}

object ProposedConfigSettings {
  val DEFAULT = new ProposedConfigSettings(new Properties)
  def fromPropertiesFile(file: File): ProposedConfigSettings = {
    val properties = new Properties()
    properties.load(new FileInputStream(file))
    new ProposedConfigSettings(properties)
  }
}

// vim: set ts=4 sw=4 et:
