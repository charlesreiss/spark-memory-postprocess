package edu.berkeley.cs.amplab.sparkmem

case class UsageInfo(
  val rddCostCurve: CostCurve,
  val broadcastCostCurve: CostCurve,
  val shuffleCostCurve: CostCurve,
  val topRddSizes: Seq[Long],
  val topAggregatorMemorySizes: Seq[Long],
  val topAggregatorDiskSizes: Seq[Long]
) {
  def rddAllSize: Long = rddCostCurve.canonicalSize
  def broadcastAllSize: Long = broadcastCostCurve.canonicalSize
  def shuffleAllSize: Long = shuffleCostCurve.canonicalSize

  def rddActiveSize(cores: Int): Long = topRddSizes.take(cores).sum
  def shuffleActiveSize(cores: Int): Long = topAggregatorMemorySizes.take(cores).sum

  import Util.bytesToString
  def summary(cores: Int): String =
    s"RDD: ${bytesToString(rddAllSize)}; Broadcast: ${bytesToString(broadcastAllSize)}; " +
    s"Shuffle [storage]: ${bytesToString(shuffleAllSize)}; " +
    s"Active: ${bytesToString(rddActiveSize(cores))} RDD + " +
    s"${bytesToString(shuffleActiveSize(cores))} shuffle"
}

