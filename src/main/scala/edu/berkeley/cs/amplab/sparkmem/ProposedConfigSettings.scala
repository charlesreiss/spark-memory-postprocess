package edu.berkeley.cs.amplab.sparkmem

import java.io.{File, FileInputStream}
import java.util.Properties

class ProposedConfigSettings(private val properties: Properties) {
  private def getLong(x: String, default: Long) = 
    Option(properties.getProperty(x)).map(_.toLong).getOrElse(default)
  private def getDouble(x: String, default: Double) = 
    Option(properties.getProperty(x)).map(_.toDouble).getOrElse(default)

  def gcBonus: Double = getDouble("gcBonus", 3.0 / 2.0)
  def assumedSlack: Double = getDouble("assumedSlack", 0.9)

  def minShuffleSizePerCore: Long = getLong("minShuffleSizePerCore", 16L * 1024L * 1024L)
  def minStorageSizePerCore: Long = getLong("minStorageSizePerCore", 16L * 1024L * 1024L)
  def minUnrollStorageSizePerCore: Long = getLong("minUnrollSizePerCore", 4L * 1024L * 1024L)

  def portionExtraStorage: Double = getDouble("portionExtraStorage", 0.45)
  def portionExtraUnrollStorage: Double = getDouble("portionExtraUnrollStorage", 0.1)
  def portionExtraShuffle: Double = getDouble("portionExtraShuffle", 0.25)
  def portionExtraUnassignedJvm: Double = getDouble("portionExtraUnassignedJvm", 0.05)
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
