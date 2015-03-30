package edu.berkeley.cs.amplab.sparkmem

case class UsageInfo(
  val rddCostCurve: CostCurve,
  val broadcastCostCurve: CostCurve,
  val shuffleCostCurve: CostCurve,
  val topRddSizes: Seq[Long],
  val topAggregatorMemorySizes: Seq[Long],
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

  import Util.bytesToString
  def summary(cores: Int): String =
    s"RDD: ${bytesToString(rddAllSize)}; Broadcast: ${bytesToString(broadcastAllSize)}; " +
    s"Shuffle [storage]: ${bytesToString(shuffleAllSize)}; " +
    s"Active: ${bytesToString(rddActiveSize(cores))} RDD + " +
    s"${bytesToString(shuffleActiveSize(cores))} shuffle; " +
    s"spilled mem ${bytesToString(totalSpilledMemory)} disk ${bytesToString(totalSpilledDisk)}"

  def csvHeader: String =
    s"cores,rdd,broadcast,shuffleStorage,rddActive,shuffleActive,spilledMem,spilledDisk,blockCount,sizeIncrements," +
    s"totalRecomputed,totalRecomputedUnknown,totalRecomputedZero,totalComputedDropped"
  def csvLine(cores: Int): String =
    s"$cores,$rddAllSize,$broadcastAllSize,$shuffleAllSize,${rddActiveSize(cores)},${shuffleActiveSize(cores)}," +
    s"$totalSpilledMemory,$totalSpilledDisk,$rddBlockCount,$rddSizeIncrements,$totalRecomputed," +
    s"$totalRecomputedUnknown,$totalRecomputedZero,$totalComputedDropped"
}

