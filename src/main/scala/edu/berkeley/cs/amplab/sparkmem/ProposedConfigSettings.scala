package edu.berkeley.cs.amplab.sparkmem

import org.json4s.jackson.Serialization

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

case class ProposedConfigSettings(
  val gcBonus: Double = 1.2,
  val assumedSlack: Double = 0.9,

  val minShuffleSizePerCore: Long = 16L * 1024L * 1024L,
  val minStorageSizePerCore: Long = 16L * 1024L * 1024L,
  val minUnrollStorageSizePerCore: Long = 4L * 1024L * 1024L,

  val portionExtraStorage: Double = 0.45,
  val portionExtraUnrollStorage: Double = 0.1,
  val portionExtraShuffle: Double = 0.25,
  val portionExtraUnassignedJvm: Double = 0.05
) {
  private implicit val jsonFormats = Serialization.formats(NoTypeHints)
  def toJson: String = Serialization.write(this)
}

object ProposedConfigSettings {
  private implicit val jsonFormats = Serialization.formats(NoTypeHints)
  val DEFAULT = ProposedConfigSettings()
  def fromJson(json: String): ProposedConfigSettings = Serialization.read[ProposedConfigSettings](json)

  def main(args: Array[String]) {
    println(DEFAULT.toJson)
  }
}

// vim: set ts=4 sw=4 et:
