package edu.berkeley.cs.amplab.sparkmem

import scala.math.Ordering

class CostCurve(data: Seq[(Long, Double)]) extends Logging {
  private val rawCurve: Array[(Long, Double)] = data.sorted.toArray

  private val maxCost = rawCurve.head._2

  private def indexAtPortion(portion: Double): Int = {
    var from = 0
    var to = rawCurve.length - 1
    val target = portion * maxCost
    while (from < to) {
      val idx = from + (to - from - 1) / 2
      val atIdx = rawCurve(idx)._2
      val atFrom = rawCurve(from)._2
      val atTo = rawCurve(to)._2
      logInfo(s"$from -> $to ($atFrom:$atIdx:$atTo) - $target")
      if (atIdx > target) {
        assert(idx != to)
        to = idx
      } else {
        from = idx + 1
      }
    }
    logInfo(s"$portion at $from")
    return from
  }

  def sizeAtPortion(portion: Double): Long = rawCurve(indexAtPortion(portion))._1

  def canonicalSize: Long = sizeAtPortion(1.0)
}
