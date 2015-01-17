package edu.berkeley.cs.amplab.sparkmem

case class UsageInfo(
  val rddCostCurve: Seq[(Long, Double)],
  val broadcastCostCurve: Seq[(Long, Double)],
  val shuffleCostCurve: Seq[(Long, Double)],
  val topRddSizes: Seq[Long],
  val topAggregatorMemorySizes: Seq[Long],
  val topAggregatorDiskSizes: Seq[Long]
) {
}

