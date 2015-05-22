package edu.berkeley.cs.amplab.sparkmem

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

case class UsageInfo(
  val rddCostCurve: CostCurve,
  val broadcastCostCurve: CostCurve,
  val shuffleCostCurve: CostCurve,
  val topRddSizes: Seq[Long],
  val topAggregatorMemorySizes: Seq[Long],
  val topAggregatorMemorySizesPairAdjust: Seq[Long],
  val topAggregatorDiskSizes: Seq[Long],

  val totalSpilledMemory: Long,
  val totalSpilledDisk: Long,
  val rddBlockCount: Long,
  val rddSizeIncrements: Long,
  val totalRecomputed: Long,
  val totalRecomputedUnknown: Long,
  val totalRecomputedZero: Long,
  val totalComputedDropped: Long
) {
  def rddAllSize: Long = rddCostCurve.canonicalSize
  def broadcastAllSize: Long = broadcastCostCurve.canonicalSize
  def shuffleAllSize: Long = shuffleCostCurve.canonicalSize

  def rddActiveSize(cores: Int): Long = topRddSizes.take(cores).sum
  def shuffleActiveSize(cores: Int): Long = topAggregatorMemorySizes.take(cores).sum
  def shuffleActiveSizeWithAdjust(cores: Int): Long = topAggregatorMemorySizesPairAdjust.take(cores).sum
  def shuffleActiveFromDiskSize(cores: Int): Long = topAggregatorDiskSizes.take(cores).sum

  import Util.bytesToString
  def summary(cores: Int): String =
    s"RDD: ${bytesToString(rddAllSize)}; Broadcast: ${bytesToString(broadcastAllSize)}; " +
    s"Shuffle [storage]: ${bytesToString(shuffleAllSize)}; " +
    s"Active: ${bytesToString(rddActiveSize(cores))} RDD + " +
    s"${bytesToString(shuffleActiveSize(cores))} shuffle; " +
    s"spilled mem ${bytesToString(totalSpilledMemory)} disk ${bytesToString(totalSpilledDisk)}"

  def csvHeader: String =
    s"cores,rdd,broadcast,shuffleStorage,rddActive,shuffleActive," +
    s"shuffleActiveAdjust,shuffleActiveDisk,spilledMem," +
    s"spilledDisk,blockCount,sizeIncrements," +
    s"totalRecomputed,totalRecomputedUnknown,totalRecomputedZero,totalComputedDropped"

  def csvLine(cores: Int): String =
    s"$cores,$rddAllSize,$broadcastAllSize,$shuffleAllSize,${rddActiveSize(cores)},${shuffleActiveSize(cores)}," +
    s"${shuffleActiveSizeWithAdjust(cores)},${shuffleActiveFromDiskSize(cores)},$totalSpilledMemory," +
    s"$totalSpilledDisk,$rddBlockCount,$rddSizeIncrements," +
    s"$totalRecomputed,$totalRecomputedUnknown,$totalRecomputedZero,$totalComputedDropped"

  def toJson: JObject = {
    import UsageInfo.{costCurveToJson, topListToJson}

    ("rddCostCurve" -> costCurveToJson(rddCostCurve)) ~
    ("broadcastCostCurve" -> costCurveToJson(rddCostCurve)) ~
    ("shuffleCostCurve" -> costCurveToJson(rddCostCurve)) ~
    ("topRddSizes" -> topListToJson(topRddSizes)) ~
    ("topAggregatorMemorySizes" -> topListToJson(topAggregatorMemorySizes)) ~
    ("topAggregatorMemorySizesPairAdjust" -> topListToJson(topAggregatorDiskSizes)) ~
    ("topAggregatorDiskSizes" -> topListToJson(topAggregatorDiskSizes)) ~
    ("totalSpilledMemory" -> totalSpilledMemory) ~
    ("totalSpilledDisk" -> totalSpilledDisk) ~
    ("rddBlockCount" -> rddBlockCount) ~
    ("rddSizeIncrements" -> rddSizeIncrements) ~
    ("totalRecomputed" -> totalRecomputed) ~
    ("totalRecomputedUnknown" -> totalRecomputedUnknown) ~
    ("totalRecomputedZero" -> totalRecomputedZero) ~
    ("totalComputedDropped" -> totalComputedDropped)
  }

  def toJsonString: String = {
    import org.json4s.jackson.JsonMethods._
    compact(render(toJson))
  }
}

object UsageInfo {
  implicit val formats = DefaultFormats 

  private[sparkmem] def costCurveToJson(costCurve: CostCurve): JValue = {
    JArray(costCurve.curvePoints.map { case (k, v) => JArray(List(JInt(k), JDouble(v))) }.toList)
  }

  private[sparkmem] def topListToJson(lst: Seq[Long]): JValue = JArray(lst.map(x => JInt(x)).toList)

  private def convertCostCurveElem(value: JValue): (Long, Double) = {
    val items = value.extract[List[JValue]]
    assert(items.size == 2)
    return (items(0).extract[Long], items(1).extract[Double])
  }
  
  private[sparkmem] def costCurveFromJson(value: JValue): CostCurve = {
    new CostCurve(value.extract[List[JValue]].map(convertCostCurveElem).toSeq)
  }

  def fromJson(json: JValue): UsageInfo = {
    UsageInfo(
      rddCostCurve = costCurveFromJson(json \ "rddCostCurve"),
      broadcastCostCurve = costCurveFromJson(json \ "broadcastCostCurve"),
      shuffleCostCurve = costCurveFromJson(json \ "shuffleCostCurve"),

      topRddSizes = (json \ "topRddSizes").extract[List[Long]],
      topAggregatorMemorySizes = (json \ "topMemorySizes").extract[List[Long]],
      topAggregatorMemorySizesPairAdjust = (json \ "topMemorySizesPairAdjust").extract[List[Long]],
      topAggregatorDiskSizes = (json \ "topAggregatorDiskSizes").extract[List[Long]],

      totalSpilledMemory = (json \ "totalSpilledMemory").extract[Long],
      totalSpilledDisk = (json \ "totalSpilledDisk").extract[Long],
      rddBlockCount = (json \ "rddBlockCount").extract[Long],
      rddSizeIncrements = (json \ "rddSizeIncrements").extract[Long],

      totalRecomputed = (json \ "totalRecomputed").extract[Long],
      totalRecomputedUnknown = (json \ "totalRecomputedUnknown").extract[Long],
      totalRecomputedZero = (json \ "totalRecomputedZero").extract[Long],
      totalComputedDropped = (json \ "totalComputedDropped").extract[Long]
    )
  }
}

