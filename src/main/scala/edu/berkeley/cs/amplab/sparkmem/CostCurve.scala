package edu.berkeley.cs.amplab.sparkmem

import scala.math.Ordering

class CostCurve(data: Seq[(Long, Double)]) {
  private val rawCurve: Array[(Long, Double)] = data.sorted.toArray

  private val maxCost = rawCurve.last._1

  private def indexAtPortion(portion: Double): Int = {
    var from = 0
    var to = rawCurve.length - 1
    val target = portion * maxCost
    while (from < to) {
      val idx = from + (to - from - 1) / 2
      val atIdx = rawCurve(idx)._2
      if (atIdx > target) {
        assert(idx != to)
        to = idx
      } else {
        from = idx + 1
      }
    }
    return from
  }

  def sizeAtPortion(portion: Double): Long = rawCurve(indexAtPortion(portion))._1

  def canonicalSize: Long = sizeAtPortion(0.99999)
}
